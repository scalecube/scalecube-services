import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._

class RequestResponseTest extends Simulation {
  // Base test parameters
  val host = System.getProperty("host", "localhost")
  val port = Integer.getInteger("port", 9090)
  val users = Integer.getInteger("users", 5000)
  val duration = Integer.getInteger("duration", 900)
  // Derived test parameters
  val rampUpDuration: Integer = duration.toInt / 3
  val scenarioDuration: Integer = 2 * duration.toInt / 3

  val url = s"ws://$host:$port"
  val echoRequest = """{"q":"/greeting/one","sid":123,"d":"echoName"}"""
  val httpConfig = http.baseURL(s"http://$host:$port")

  System.out.println("Server address: " + url)
  System.out.println("Users count: " + users)
  System.out.println("Simulation duration: " + duration + " seconds")
  System.out.println("Scenario rampup: " + rampUpDuration + " seconds")
  System.out.println("Scenario duration: " + scenarioDuration + " seconds")

  // User scenario is a chain of requests and pauses
  val usersScn = {
    scenario("RequestResponseTest")
      .exec(ws("socket").open(url))
      .pause(2)
      .during(scenarioDuration seconds) {
        exec(ws("Echo Request")
          .sendText(echoRequest)
          .check(wsAwait.within(3 seconds).until(1).regex( """.*Echo.*""")))
          .pause(1)
      }
      .pause(1)
      .exec(ws("close").close)
  }

  // Start user scenarios
  setUp(usersScn.inject(
    rampUsers(users) over (rampUpDuration seconds)))
    .protocols(httpConfig)
    .maxDuration(duration)
}
