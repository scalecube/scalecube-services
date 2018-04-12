package io.scalecube.services.a.b.testing;

import io.scalecube.services.Microservices;


public class ServiceTagsExample {

  public static void main(String[] args) {
    Microservices gateway = Microservices.builder().build();

    Microservices services1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services().service(new GreetingServiceImplA()).tag("Weight", "0.3").add()
        .build()
        .build();

    Microservices services2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services().service(new GreetingServiceImplB()).tag("Weight", "0.7").add()
        .build()
        .build();

    CanaryService service = gateway.call()
        .router(gateway.router(CanaryTestingRouter.class))
        .api(CanaryService.class);

    for (int i = 0; i < 10; i++) {
      service.greeting("joe").whenComplete((success, error) -> {
        success.startsWith("B");
        System.out.println(success);
      });
    }
  }

}
