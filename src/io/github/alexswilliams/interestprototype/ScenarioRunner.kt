package io.github.alexswilliams.interestprototype

import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import kotlin.math.pow

object ScenarioRunner {
    fun runScenario(firstDay: LocalDate, lastDay: LocalDate, actions: Map<LocalDate, List<DailyAction>>) {
        val dates = firstDay.datesUntil(lastDay).toList().toList()
        dates.forEach { today ->
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
        for (action in actions) {
            when (action) {
                is DailyAction.CreateAccount -> {}
                is DailyAction.CloseAccount -> {}
                is DailyAction.CreatePeriod -> {}
                is DailyAction.ConsumeEoDBalanceFile -> readEodBalanceFile(yesterday, action.balances)
            }
        }
        processEodResults(yesterday)
    }

    private fun readEodBalanceFile(forDate: LocalDate, balances: Map<Int, BigDecimal>) {
        println(balances)
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

    private fun processEodResults(currentDay: LocalDate) {
        val previousDay = currentDay.minusDays(1)
        val completions = Database.getAllEodBalanceCompletions()

        for (event in completions) {
            val currentAccountBalance = Database.getEodBalance(event.eodBalanceId)
            val currentPeriod = Database.findPeriodForAccountOnDate(event.accountId, currentDay)
                ?: throw Error("Account not in period")
            val currentSchemeVersion = Database.findSchemeVersionForDate(currentPeriod.schemeId, currentDay)
                ?: throw Error("Scheme has no version on date $currentDay")

            val previousAccountBalance = Database.findEodBalanceForDate(event.accountId, previousDay)
            val previousPeriod = Database.findPeriodForAccountOnDate(event.accountId, previousDay)
            val previousSchemeVersion = previousPeriod?.let { Database.findSchemeVersionForDate(it.schemeId, previousDay) }
            val previousAccrualRun = Database.findAccrualForDate(event.accountId, previousDay)

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
                    val previousAccrualBalance = previousAccrualRun?.endAccrualBalance ?: 0.0
                    val currentAccrualBalance = compound(
                        aer = currentSchemeVersion.aer,
                        start = currentDay,
                        end = currentDay,
                        startingBalance = currentAccountBalance.balance.toDouble() + previousAccrualBalance
                    ) - currentAccountBalance.balance.toDouble()
                    val accrualRunId = Database.createNewAccrualRun(
                        accountId = event.accountId,
                        periodId = currentPeriod.id,
                        schemeVersionId = currentSchemeVersion.id,
                        eodBalanceId = currentAccountBalance.id,
                        on = currentDay,
                        startingBalance = previousAccrualBalance,
                        closingBalance = currentAccrualBalance
                    )
                    Triple(previousAccrualBalance, currentAccrualBalance, accrualRunId)
                } else {
                    if (previousAccrualRun == null) throw Error("Expecting previous accrual run to exist for account ${event.accountId} on $currentDay")
                    if (previousAccrualRun.end != previousDay) throw Error("Expected existing accrual run to end the day before the date currently being processed")
                    val previousAccrualBalance = compound(
                        aer = currentSchemeVersion.aer,
                        start = previousAccrualRun.start,
                        end = previousDay,
                        startingBalance = currentAccountBalance.balance.toDouble() + previousAccrualRun.startAccrualBalance
                    ) - currentAccountBalance.balance.toDouble()
                    val currentAccrualBalance = compound(
                        aer = currentSchemeVersion.aer,
                        start = previousAccrualRun.start,
                        end = currentDay,
                        startingBalance = currentAccountBalance.balance.toDouble() + previousAccrualRun.startAccrualBalance
                    ) - currentAccountBalance.balance.toDouble()
                    Database.extendExistingAccrualRun(previousAccrualRun.id, currentDay, currentAccrualBalance)
                    Triple(previousAccrualBalance, currentAccrualBalance, previousAccrualRun.id)
                }

            val accruedToday = currentAccrualBalance - previousAccrualBalance
            Database.enqueueDailyAccrual(event.accountId, nextAccrualRunId, currentDay, accruedToday.roundDown2dp())

            Database.completeEodBalanceCompletionEvent(event.accountId, currentDay)
        }
    }

    private fun Double.roundDown2dp() = BigDecimal(this, MathContext.UNLIMITED).setScale(2, RoundingMode.DOWN)

    private fun compound(aer: BigDecimal, start: LocalDate, end: LocalDate, startingBalance: Double): Double {
        val daysIntoRun: Long = start.until(end, ChronoUnit.DAYS) + 1
        // TODO: leap years, also this isn't a real calculation, just something to give vague numbers for the poc
        return if (daysIntoRun == 365L) (1 + aer.toDouble()) * startingBalance
        else startingBalance * (1 + aer.toDouble()).pow(daysIntoRun.toDouble() / 365)
    }
}
