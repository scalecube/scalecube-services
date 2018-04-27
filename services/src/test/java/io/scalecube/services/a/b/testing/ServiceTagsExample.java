package io.scalecube.services.a.b.testing;

import io.scalecube.services.GreetingRequest;
import io.scalecube.services.Microservices;

import reactor.core.publisher.Mono;


public class ServiceTagsExample {

  public static void main(String[] args) {
    Microservices gateway = Microservices.builder().build();

    Microservices services1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .withService(new GreetingServiceImplA()).withTag("Weight", "0.3").register()
        .build();

    Microservices services2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .withService(new GreetingServiceImplB()).withTag("Weight", "0.7").register()
        .build();

    CanaryService service = gateway.call()
        .router(gateway.router(CanaryTestingRouter.class))
        .api(CanaryService.class);

    for (int i = 0; i < 10; i++) {
      Mono.from(service.greeting(new GreetingRequest("joe"))).doOnNext(success -> {
        success.getResult().startsWith("B");
        System.out.println(success);
      });
    }
  }

}
