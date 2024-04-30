package io.github.alexswilliams.interestprototype

import java.math.BigDecimal
import java.time.LocalDate

object Database {
    /*
        data class ProductRow(val id: Int, val defaultDepositSchemeId: Int, val name: String)
        data class DepositSchemeRow(val id: Int)
    */
    data class DepositSchemeVersion(val id: Int, val schemeId: Int, val from: LocalDate, val aer: BigDecimal)
    data class AccountRow(val id: Int, val createdOn: LocalDate, val productId: Int)
    data class AccountClosureRow(val id: Int, val accountId: Int, val closedOn: LocalDate)
    data class PeriodRow(val id: Int, val schemeId: Int, val accountId: Int, val periodStart: LocalDate, val periodEnd: LocalDate)
    data class EodBalanceRow(val id: Int, val accountId: Int, val runStart: LocalDate, var runEnd: LocalDate, val balance: BigDecimal)
    enum class EodActionTaken { NEW_BALANCE_RUN_CREATED, EXISTING_BALANCE_RUN_EXTENDED }
    data class EodBalanceCompletionQueueRow(val eodBalanceId: Int, val accountId: Int, val date: LocalDate, val actionTaken: EodActionTaken)
    data class AccrualRow(
        val id: Int,
        val accountId: Int,
        val periodId: Int,
        val schemeVersionId: Int,
        val eodBalanceId: Int,
        val start: LocalDate,
        var end: LocalDate,
        val startAccrualBalance: Double,
        var endAccrualBalance: Double,
    )

    data class DailyAccrualCompletionQueueRow(val accountId: Int, val accrualRunId: Int, val on: LocalDate, val delta: BigDecimal)
    data class AccrualLedgerRow(val id: Int, val productId: Int, val valueDate: LocalDate, val amount: BigDecimal)
    data class PaymentRow(val id: Int, val accountId: Int, val periodId: Int, val amount: BigDecimal, val on: LocalDate)

    private val depositSchemeVersionTable: MutableList<DepositSchemeVersion> = mutableListOf(
        DepositSchemeVersion(1, 1, LocalDate.parse("2020-01-01"), BigDecimal("0.035")),
        DepositSchemeVersion(2, 2, LocalDate.parse("2020-01-01"), BigDecimal("0.05")),
        DepositSchemeVersion(3, 1, LocalDate.parse("2024-04-20"), BigDecimal("0.001")),
    )

    fun findSchemeVersion(id: Int) = depositSchemeVersionTable.first { it.id == id }
    fun findSchemeVersionForDate(schemeId: Int, on: LocalDate) =
        depositSchemeVersionTable.filter { it.id == schemeId }.lastOrNull { it.from <= on }


    private val accountTable: MutableList<AccountRow> = mutableListOf()
    fun createAccount(id: Int, on: LocalDate, productId: Int) = accountTable.add(AccountRow(id, on, productId))
    fun findAccount(id: Int) = accountTable.first { it.id == id }


    private val accountClosureTable: MutableList<AccountClosureRow> = mutableListOf()
    fun closeAccount(accountId: Int, on: LocalDate) =
        accountClosureTable.add(AccountClosureRow(1 + (accountClosureTable.maxOfOrNull { it.id } ?: 0), findAccount(accountId).id, on))

    fun isClosed(accountId: Int) = accountClosureTable.any { it.accountId == accountId }


    private val periodTable: MutableList<PeriodRow> = mutableListOf()
    fun findPeriod(id: Int) = periodTable.first { it.id == id }
    fun openPeriod(accountId: Int, schemeVersionId: Int, start: LocalDate, end: LocalDate) =
        periodTable.add(PeriodRow(1 + (periodTable.maxOfOrNull { it.id } ?: 0), schemeVersionId, accountId, start, end))

    fun findPeriodForAccountOnDate(accountId: Int, on: LocalDate) =
        periodTable.filter { it.accountId == accountId }.find { it.periodStart <= on && it.periodEnd >= on }


    private val eodBalanceTable: MutableList<EodBalanceRow> = mutableListOf()
    fun getEodBalance(id: Int) = eodBalanceTable.first { it.id == id }
    fun findEodBalanceForDate(accountId: Int, on: LocalDate) =
        eodBalanceTable.filter { it.accountId == accountId }.firstOrNull { it.runStart <= on && it.runEnd >= on }

    fun findLatestEodBalanceForAccount(accountId: Int): EodBalanceRow? =
        eodBalanceTable.filter { it.accountId == findAccount(accountId).id }.maxByOrNull { it.runStart }

    fun createEodBalanceForAccount(accountId: Int, balance: BigDecimal, on: LocalDate): Int =
        (1 + (eodBalanceTable.maxOfOrNull { it.id } ?: 0))
            .also { newId -> eodBalanceTable.add(EodBalanceRow(newId, findAccount(accountId).id, on, on, balance)) }

    fun extendEodBalanceRun(id: Int, newRunEnd: LocalDate) {
        eodBalanceTable.first { it.id == id }.runEnd = newRunEnd
    }


    private val eodBalanceCompletionQueue: MutableList<EodBalanceCompletionQueueRow> = mutableListOf()
    fun getAllEodBalanceCompletions() = eodBalanceCompletionQueue.toList()
    fun enqueueEodBalanceCompletion(eodBalanceId: Int, actionTaken: EodActionTaken) = with(getEodBalance(eodBalanceId)) {
        eodBalanceCompletionQueue.add(EodBalanceCompletionQueueRow(id, accountId, runEnd, actionTaken))
    }

    fun completeEodBalanceCompletionEvent(accountId: Int, on: LocalDate) {
        eodBalanceCompletionQueue.removeAll { it.accountId == accountId && it.date == on }
    }

    private val accrualTable: MutableList<AccrualRow> = mutableListOf()
    fun findAccrualForDate(accountId: Int, on: LocalDate): AccrualRow? =
        accrualTable.filter { it.accountId == accountId && it.start <= on && it.end >= on }.maxByOrNull { it.start }

    fun createNewAccrualRun(
        accountId: Int,
        periodId: Int,
        schemeVersionId: Int,
        eodBalanceId: Int,
        on: LocalDate,
        startingBalance: Double,
        closingBalance: Double,
    ): Int {
        val newId = 1 + (accrualTable.maxOfOrNull { it.id } ?: 0)
        accrualTable.add(
            AccrualRow(
                id = newId,
                accountId = accountId,
                periodId = periodId,
                schemeVersionId = schemeVersionId,
                eodBalanceId = eodBalanceId,
                start = on,
                end = on,
                startAccrualBalance = startingBalance,
                endAccrualBalance = closingBalance,
            )
        )
        return newId
    }

    fun extendExistingAccrualRun(id: Int, newRunEnd: LocalDate, newEndBalance: Double) =
        with(accrualTable.first { it.id == id }) {
            this.end = newRunEnd
            this.endAccrualBalance = newEndBalance
        }

    private val dailyAccrualCompletionQueueRow: MutableList<DailyAccrualCompletionQueueRow> = mutableListOf()
    fun enqueueDailyAccrual(accountId: Int, accrualRunId: Int, forDate: LocalDate, accruedToday: BigDecimal) =
        dailyAccrualCompletionQueueRow.add(DailyAccrualCompletionQueueRow(accountId, accrualRunId, forDate, accruedToday))

    fun findEarliestAccrualResult() = dailyAccrualCompletionQueueRow.minOfOrNull { it.on }
    fun findAccrualResultsForDay(on: LocalDate) = dailyAccrualCompletionQueueRow.filter { it.on == on }
    fun removeAccrualResult(accrualRunId: Int, on: LocalDate) =
        dailyAccrualCompletionQueueRow.removeAll { it.accrualRunId == accrualRunId && it.on == on }

    private val accrualLedgerTable: MutableList<AccrualLedgerRow> = mutableListOf()
    fun postEntry(productId: Int, amount: BigDecimal, on: LocalDate) =
        accrualLedgerTable.add(AccrualLedgerRow((1 + (accrualLedgerTable.maxOfOrNull { it.id } ?: 0)), productId, on, amount))

    private val paymentTable: MutableList<PaymentRow> = mutableListOf()
    fun createPayment(accountId: Int, periodId: Int, amount: BigDecimal, on: LocalDate) =
        paymentTable.add(PaymentRow(1 + (paymentTable.maxOfOrNull { it.id } ?: 0), accountId, periodId, amount, on))

    fun printState(day: LocalDate, accountFilter: (Int) -> Boolean = { _ -> true }) {
        println(day)
        print("Scheme Versions:\n"); println(depositSchemeVersionTable)
        print("Accounts:\n"); println(accountTable.filter { accountFilter(it.id) })
        print("Account Closures:\n"); println(accountClosureTable.filter { accountFilter(it.accountId) })
        print("EoD Balances:\n"); println(eodBalanceTable.filter { accountFilter(it.accountId) }.joinToString("\n"))
        print("EoD Actions:\n"); println(eodBalanceCompletionQueue.filter { accountFilter(it.accountId) }.joinToString("\n"))
        print("Periods:\n"); println(periodTable.filter { accountFilter(it.accountId) }.joinToString("\n"))
        print("Accruals:\n"); println(accrualTable.filter { accountFilter(it.accountId) }.joinToString("\n"))
        print("Payments:\n"); println(paymentTable.filter { accountFilter(it.accountId) }.joinToString("\n"))
        print("Accrual Actions:\n"); println(dailyAccrualCompletionQueueRow.filter { accountFilter(it.accountId) }.joinToString("\n"))
        print("Ledger Entries:\n"); println(accrualLedgerTable.joinToString("\n"))
        println()
    }
}
