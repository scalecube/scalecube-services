import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._

class RequestResponseTest extends Simulation {
  // Base test parameters
  val host = System.getProperty("host", "127.0.0.1")
  val port = Integer.getInteger("port", 8080)
  val users = Integer.getInteger("users", 5000)
  val duration = Integer.getInteger("duration", 120)
  // Derived test parameters
  val rampUpDuration: Integer = duration.toInt / 10
  val scenarioDuration: Integer = 2 * duration.toInt / 10

  // Scenario parameters
  val echoRequest = """"echoName""""
  val url = s"ws://$host:$port/greeting/one"
  val httpConfig = http.baseURL(s"http://$host:$port")

  // User scenario is a chain of requests and pauses
  val usersScn = {
    scenario("RequestResponseTest")
      .exec(ws("socket").open(url)
      )
      .pause(2 seconds)
      .during(scenarioDuration seconds) {
        exec(ws("Echo Request").sendText(echoRequest).check(wsAwait.within(3000 milliseconds).until(1).regex( """(.*)echoName""").exists)).pause(1000 milliseconds)
      }
      .exec(ws("close").close)
  }

  // Start user scenarios
  setUp(usersScn.inject(
    rampUsers(users) over (rampUpDuration seconds))
  ).protocols(httpConfig)
}
