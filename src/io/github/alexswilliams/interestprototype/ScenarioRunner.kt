package io.github.alexswilliams.interestprototype

import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import kotlin.math.pow

object ScenarioRunner {
    fun runScenario(firstDay: LocalDate, lastDay: LocalDate, actions: Map<LocalDate, List<DailyAction>>) {
        for (today in firstDay.datesUntil(lastDay.plusDays(1))) {
            runNightlyActionsThatProcessYesterday(today.minusDays(1), actions[today.minusDays(1)].orEmpty())
            runDailyActionsThatProcessToday(today, actions[today].orEmpty())
            Database.printState(today) { it == 6 }
        }
    }

    private fun runDailyActionsThatProcessToday(today: LocalDate, actions: List<DailyAction>) {
        for (action in actions) {
            when (action) {
                is DailyAction.CreateAccount -> Database.createAccount(action.id, today, action.productId)
                is DailyAction.CloseAccount -> Database.closeAccount(action.id, today)
                is DailyAction.CreatePeriod ->
                    // TODO: closing old periods
                    Database.openPeriod(
                        accountId = action.accountId,
                        schemeVersionId = Database.findSchemeVersionForDate(action.schemeId, today)!!.id,
                        start = action.start,
                        end = action.end
                    )

                is DailyAction.ConsumeEoDBalanceFile -> {}
            }
        }
    }

    private fun runNightlyActionsThatProcessYesterday(yesterday: LocalDate, actions: List<DailyAction>) {
        actions.filterIsInstance<DailyAction.ConsumeEoDBalanceFile>().firstOrNull()
            ?.let { action -> readEodBalanceFile(yesterday, action.balances) }

        processEodBalanceResults(yesterday)
        processAccrualResults(yesterday)
    }

    private fun readEodBalanceFile(forDate: LocalDate, balances: Map<Int, BigDecimal>) {
        for ((accountId, balance) in balances) {
            if (Database.isClosed(accountId)) throw Error("Tried to submit EoD Balance for closed account: $forDate, $accountId")
            val currentBalanceRow = Database.findLatestEodBalanceForAccount(accountId)
            if (currentBalanceRow == null || currentBalanceRow.balance != balance) {
                val newId = Database.createEodBalanceForAccount(accountId, balance, forDate)
                Database.enqueueEodBalanceCompletion(newId, Database.EodActionTaken.NEW_BALANCE_RUN_CREATED)

            } else if (currentBalanceRow.runEnd != forDate.minusDays(1)) {
                // TODO: This eod balance should be DLQ'd and retried
                throw Error("Had to DLQ an EoD Balance")

            } else {
                Database.extendEodBalanceRun(currentBalanceRow.id, forDate)
                Database.enqueueEodBalanceCompletion(currentBalanceRow.id, Database.EodActionTaken.EXISTING_BALANCE_RUN_EXTENDED)
            }
        }
    }

    private fun processEodBalanceResults(yesterday: LocalDate) {
        fun compound(aer: BigDecimal, start: LocalDate, end: LocalDate, startingBalance: Double): Double {
            val daysIntoRun: Long = start.until(end, ChronoUnit.DAYS) + 1
            return if (daysIntoRun == 365L) (1 + aer.toDouble()) * startingBalance // maybe this could avoid the nasty rounding problem?
            else startingBalance * (1 + aer.toDouble()).pow(daysIntoRun.toDouble() / 365)
        }

        val dayBefore = yesterday.minusDays(1)
        val completions = Database.getAllEodBalanceCompletions()

        for (event in completions) {
            val currentAccountBalance = Database.getEodBalance(event.eodBalanceId)
            val currentPeriod = Database.findPeriodForAccountOnDate(event.accountId, yesterday)
                ?: throw Error("Account not in period")
            val currentSchemeVersion = Database.findSchemeVersionForDate(currentPeriod.schemeId, yesterday)
                ?: throw Error("Scheme has no version on date $yesterday")

            val previousAccountBalance = Database.findEodBalanceForDate(event.accountId, dayBefore)
            val previousPeriod = Database.findPeriodForAccountOnDate(event.accountId, dayBefore)
            val previousSchemeVersion = previousPeriod?.let { Database.findSchemeVersionForDate(it.schemeId, dayBefore) }
            val previousAccrualRun = Database.findAccrualForDate(event.accountId, dayBefore)

            val newAccrualRunNeeded =
                previousAccountBalance == null
                        || previousPeriod == null
                        || previousSchemeVersion == null
                        || previousAccrualRun == null
                        || currentAccountBalance != previousAccountBalance
                        || currentPeriod != previousPeriod
                        || currentSchemeVersion != previousSchemeVersion

            val (previousAccrualBalance, currentAccrualBalance, nextAccrualRunId) =
                if (newAccrualRunNeeded) {
                    val previousEndAccrual = previousAccrualRun?.endAccrualBalance ?: 0.0
                    val startingAccrualBalance =
                        if (previousPeriod?.periodEnd == dayBefore) previousEndAccrual.roundDown2dp().toDouble() - previousEndAccrual
                        else previousEndAccrual
                    val currentAccrualBalance = compound(
                        aer = currentSchemeVersion.aer,
                        start = yesterday,
                        end = yesterday,
                        startingBalance = currentAccountBalance.balance.toDouble() + startingAccrualBalance
                    ) - currentAccountBalance.balance.toDouble()
                    val accrualRunId = Database.createNewAccrualRun(
                        accountId = event.accountId,
                        periodId = currentPeriod.id,
                        schemeVersionId = currentSchemeVersion.id,
                        eodBalanceId = currentAccountBalance.id,
                        on = yesterday,
                        startingBalance = startingAccrualBalance,
                        closingBalance = currentAccrualBalance
                    )
                    Triple(startingAccrualBalance, currentAccrualBalance, accrualRunId)
                } else {
                    if (previousAccrualRun == null) throw Error("Expecting previous accrual run to exist for account ${event.accountId} on $yesterday")
                    if (previousAccrualRun.end != dayBefore) throw Error("Expected existing accrual run to end the day before the date currently being processed")
                    val previousAccrualBalance = compound(
                        aer = currentSchemeVersion.aer,
                        start = previousAccrualRun.start,
                        end = dayBefore,
                        startingBalance = currentAccountBalance.balance.toDouble() + previousAccrualRun.startAccrualBalance
                    ) - currentAccountBalance.balance.toDouble()
                    val currentAccrualBalance = compound(
                        aer = currentSchemeVersion.aer,
                        start = previousAccrualRun.start,
                        end = yesterday,
                        startingBalance = currentAccountBalance.balance.toDouble() + previousAccrualRun.startAccrualBalance
                    ) - currentAccountBalance.balance.toDouble()
                    Database.extendExistingAccrualRun(previousAccrualRun.id, yesterday, currentAccrualBalance)
                    Triple(previousAccrualBalance, currentAccrualBalance, previousAccrualRun.id)
                }

            val accruedToday = currentAccrualBalance - previousAccrualBalance
            Database.enqueueDailyAccrual(event.accountId, nextAccrualRunId, yesterday, accruedToday.roundDown2dp())

            if (currentPeriod.periodEnd == yesterday) {
                generatePayment(currentPeriod, currentAccrualBalance, yesterday.plusDays(1))
            }

            Database.completeEodBalanceCompletionEvent(event.accountId, yesterday)
        }
    }

    private fun generatePayment(period: Database.PeriodRow, currentAccrualBalance: Double, paymentDate: LocalDate) {
        // TODO: presumably also some ledger stuff
        Database.createPayment(period.accountId, period.id, currentAccrualBalance.roundDown2dp(), paymentDate)
    }

    private fun processAccrualResults(yesterday: LocalDate) {
        val startDate = Database.findEarliestAccrualResult() ?: return
        for (date in startDate.datesUntil(yesterday.plusDays(1))) {
            val resultsForDay = Database.findAccrualResultsForDay(date)

            // Would probably happen in the db?
            resultsForDay.groupBy { Database.findAccount(it.accountId).productId }
                .mapValues { (_, accrualsForProduct) -> accrualsForProduct.sumOf { it.delta } }
                .forEach { (productId, total) -> Database.postEntry(productId, total, yesterday) }

            resultsForDay.forEach { Database.removeAccrualResult(it.accrualRunId, it.on) }
        }
    }

    private fun Double.roundDown2dp() = BigDecimal(this, MathContext.UNLIMITED).setScale(2, RoundingMode.DOWN)
}
