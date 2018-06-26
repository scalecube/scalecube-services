import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

class RequestStreamTest extends Simulation {
  // Base test parameters
  val host = System.getProperty("host", "localhost")
  val port = Integer.getInteger("port", 9090)
  val users = Integer.getInteger("users", 1000)
  val duration = Integer.getInteger("duration", 900)
  val count: Integer = Integer.getInteger("count", 1000)

  // Derived test parameters: 1/10 - rampup, 8/10 - load, 1/10 - rampdown
  val scenarioDuration: Integer = 2 * duration.toInt / 3
  val rampUpDuration: Integer = if (duration.toInt >= 3) duration.toInt / 3 else 1

  // Scenario parameters
  val url = s"ws://$host:$port"
  val data = s"""$count"""
  val echoRequest = s"""{"q":"/greeting/manyStream","d":$data}"""
  val httpConfig = http.baseURL(s"http://$host:$port")

  System.out.println("Server address: " + url)
  System.out.println("Users count: " + users)
  System.out.println("Simulation duration: " + duration + " seconds")
  System.out.println("Scenario rampup: " + rampUpDuration + " seconds")
  System.out.println("Scenario duration: " + scenarioDuration + " seconds")
  System.out.println("Scenario count: " + count + " responses")


  // User scenario is a chain of requests and pauses
  val usersScn = {
    scenario("RequestResponseTest")
      .exec(ws("socket").open(url))
      .pause(2)
      .during(scenarioDuration seconds) {
        exec(ws("Echo Request")
          .sendText(echoRequest)
          .check(wsAwait.within(rampUpDuration seconds).until(count).regex( """.*manyStream.*""")))
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
