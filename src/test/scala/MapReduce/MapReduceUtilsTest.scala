package MapReduce

import HelperUtils.{MapReduceUtils, Parameters}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.Console.in

class MapReduceUtilsTest extends AnyFlatSpec with Matchers with PrivateMethodTester {

  behavior of "MapReduceUtils"

  "isValidLogLevel" should "output true when a valid log level is passed" in {
    val str1 = "INFO"
    val str2 = "DEBUG"
    val str3 = "DANGER"

    val ans1 = MapReduceUtils.isValidLogLevel(str1)
    val ans2 = MapReduceUtils.isValidLogLevel(str2)
    val ans3 = MapReduceUtils.isValidLogLevel(str3)

    assert(ans1)
    assert(ans2)
    assert(!ans3)

  }

  "getLogLevel" should "Return the correctr log level from the log message" in {
    val msg1 = "16:54:04.774 [scala-execution-context-global-25] ERROR HelperUtils.Parameters$ - s%]s,+2k|D}K7b/XCwG&@7HDPR8z"
    val msg2 = "16:54:05.301 [scala-execution-context-global-25] DEBUG HelperUtils.Parameters$ - JrQB;P0\"&+6;&Dk-"
    val ans1 = MapReduceUtils.getLogLevel(msg1)
    val ans2 = MapReduceUtils.getLogLevel(msg2)

    assert(ans1 == "ERROR")
    assert(ans2 == "DEBUG")
  }

  "getLogMessage" should "Return the correct log message from the log input" in {
    val msg1 = "16:54:04.774 [scala-execution-context-global-25] ERROR HelperUtils.Parameters$ - s%]s,+2k|D}K7b/XCwG&@7HDPR8z"
    val msg2 = "16:54:05.301 [scala-execution-context-global-25] DEBUG HelperUtils.Parameters$ - JrQB;P0\"&+6;&Dk-"
    val ans1 = MapReduceUtils.getLogMessage(msg1)
    val ans2 = MapReduceUtils.getLogMessage(msg2)

    assert(ans1 == "s%]s,+2k|D}K7b/XCwG&@7HDPR8z")
    assert(ans2 == "JrQB;P0\"&+6;&Dk-")
  }

  "doesContainPattern" should "identify if the given string has the regex pattern" in {
    val msg1 = "22:23:19.384 [scala-execution-context-global-25] ERROR HelperUtils.Parameters$ - N&I3aq7Wae3A9fQ5ice1V5k~=R{s6ng"
    val msg2 = "16:54:05.301 [scala-execution-context-global-25] DEBUG HelperUtils.Parameters$ - JrQB;P0\"&+6;&Dk-"

    val ans1 = MapReduceUtils.doesContainPattern(msg1, Parameters.generatingPattern.r)
    val ans2 = MapReduceUtils.doesContainPattern(msg2, Parameters.generatingPattern.r)

    assert(ans1)
    assert(!ans2)
  }

  "getLogTimeStamp" should "return timestamp string from the log message" in {
    val msg1 = "22:23:19.384 [scala-execution-context-global-25] ERROR HelperUtils.Parameters$ - N&I3aq7Wae3A9fQ5ice1V5k~=R{s6ng"
    val msg2 = "16:54:05.301 [scala-execution-context-global-25] DEBUG HelperUtils.Parameters$ - JrQB;P0\"&+6;&Dk-"

    val ans1 = MapReduceUtils.getLogTimeStamp(msg1)
    val ans2 = MapReduceUtils.getLogTimeStamp(msg2)

    assert(ans1.get == "22:23:19.384")
    assert(ans2.get == "16:54:05.301")
  }

}
