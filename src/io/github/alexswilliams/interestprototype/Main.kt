package io.github.alexswilliams.interestprototype

import io.github.alexswilliams.interestprototype.ScenarioRunner.runScenario
import java.math.BigDecimal
import java.time.LocalDate

private val TODAY: LocalDate = LocalDate.parse("2024-04-25")
private const val SCENARIOS_INPUT: String = """
    Account ID, Product ID, Opened, Closed, Balance History
    1, 1, 2024-03-25, still open, [2024-03-25, 1000.00] // opened normally, excludes leap day
    2, 2, 2024-02-29, still open, [2024-02-29, 1000.00] // opened normally, opened on leap day
    3, 2, 2024-02-25, still open, [2024-02-25, 1000.00] // opened normally, includes leap day
    4, 1, 2024-04-01, still open, [2024-04-01, 5000.00, 2024-04-02, 10_000.00, 2024-04-03, 15_000.00] // funded over multiple days
    5, 1, 2024-04-01, 2024-04-13, [2024-04-01, 1500.00] // closed in the 14-day window - TODO: do you get the final 0.00 eod balance through?
    6, 1, 2023-02-05, 2024-02-05, [2023-02-05, 50_000.00] // closed by maturity
    """

sealed class DailyAction {
    data class ConsumeEoDBalanceFile(val balances: Map<Int, BigDecimal>) : DailyAction()
    data class CreateAccount(val id: Int, val productId: Int) : DailyAction()
    data class CloseAccount(val id: Int) : DailyAction()
    data class CreatePeriod(val accountId: Int, val start: LocalDate, val end: LocalDate, val schemeId: Int) : DailyAction()
}

private fun main() {
    data class DailyBalance(val date: LocalDate, val balance: BigDecimal)
    data class AccountHistory(
        val id: Int,
        val product: Int,
        val opened: LocalDate,
        val closed: LocalDate?,
        val balanceHistory: List<DailyBalance>,
    )

    val scenarios = SCENARIOS_INPUT.replace(Regex("(//.*$)|_|\\[|]", RegexOption.MULTILINE), "")
        .lines().filter { it.isNotBlank() }.drop(1)
        .map { line -> line.split(',').map { it.trim() } }
        .map { line ->
            AccountHistory(
                id = line[0].toInt(),
                product = line[1].toInt(),
                opened = date(line[2]),
                closed = if (line[3] == "still open") null else date(line[3]),
                balanceHistory = line.drop(4)
                    .windowed(2, 2) { (date, balance) -> DailyBalance(date(date), BigDecimal(balance)) }
                    .sortedBy { it.date }
            )
        }
    val firstDay = scenarios.minOf { it.opened }

    val accountOpenings = scenarios.map { it.opened to DailyAction.CreateAccount(it.id, it.product) }
    val accountClosures = scenarios.filter { it.closed != null }.map { it.closed!! to DailyAction.CloseAccount(it.id) }
    val endOfDayBalanceFiles = firstDay.datesUntil(TODAY).toList().map { day ->
        day to DailyAction.ConsumeEoDBalanceFile(
            scenarios.filter { (it.opened <= day) && (it.closed == null || it.closed > day) }
                .associate { account -> account.id to account.balanceHistory.last { it.date <= day }.balance }
        )
    }
    val periodCreations = scenarios.map { it.opened to DailyAction.CreatePeriod(it.id, it.opened, it.opened.plusYears(1), 1) }

    runScenario(
        firstDay = firstDay,
        lastDay = TODAY,
        actions = (accountOpenings + accountClosures + endOfDayBalanceFiles + periodCreations)
            .groupBy({ it.first }) { it.second })
}

fun date(str: String): LocalDate = LocalDate.parse(str)
