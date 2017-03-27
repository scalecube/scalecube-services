package io.scalecube.examples.services.apigateway.http;

import io.scalecube.examples.services.GreetingService;
import io.scalecube.examples.services.GreetingServiceImpl;
import io.scalecube.services.Microservices;

import org.rapidoid.io.IO;
import org.rapidoid.setup.On;


/**
 * Example of HTTP API Gateway pattern.
 */
public class HttpApiGateway {

  /**
   * API Gateway example demonstrate the basic concept of an API Gateway "adapter" calling a microservice. 
   * the example reates a micro-cluster consists of:
   * <li>gateway node with API Gateway listening on HTTP port 8080. 
   * <li>GreetingService node.
   * 
   * <p>HTTP listener receives a GET request from http://localhost:8080/?name=joe.
   * 
   * <p>using a service proxy it calls provider node of GreetingService.
   * 
   */
  public static void main(String[] args) throws Exception {

    // Create Micro-cluster for the api gateway cluster Member
    Microservices gateway = Microservices.builder().build();

    // Create Micro-cluster for the service provider
    Microservices serviceProvider = Microservices.builder()
        // serviceProvider will join the gateway micro-cluster
        .seeds(gateway.cluster().address())
        // this Micro-cluster provision GreetingService microservice instance
        .services(new GreetingServiceImpl())
        .build();


    // Create service proxy from gateway micro-cluster.
    GreetingService service = gateway.proxy()
        .api(GreetingService.class)
        .create();

    // HTTP Gateway using rapidoid API.
    On.port(8080).route("GET", "/").plain(req -> {
      req.async(); // this is async request

      // extract the name from the name query param
      // http://localhost:8080/?name=joe
      String name = req.param("name");
      service.greeting(name).whenComplete((greetingResponse, ex) -> {
        if (ex == null) {
          // when async response comes back from greeting service write a reply to client.
          IO.write(req.response().out(), greetingResponse);
        }
        req.done();
      });
      return req;
    });
  }
}
