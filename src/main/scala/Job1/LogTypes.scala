package Job1

/**
 * This class is used to aggregate the different types of log messages and the
 * number of matched with the regex pattern for each log message type
 * @param warn
 * @param info
 * @param debug
 * @param error
 * @param warnMatch
 * @param infoMatch
 * @param debugMatch
 * @param errorMatch
 */
class LogTypes(val warn: Int, val info: Int, val debug: Int, val error: Int,
               val warnMatch: Int, val infoMatch: Int, val debugMatch: Int, val errorMatch: Int)
