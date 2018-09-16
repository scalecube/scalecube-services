package io.scalecube.services.routings;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.routings.sut.CanaryService;
import io.scalecube.services.routings.sut.GreetingServiceImplA;
import io.scalecube.services.routings.sut.GreetingServiceImplB;
import io.scalecube.services.routings.sut.WeightedRandomRouter;
import io.scalecube.services.sut.GreetingRequest;
import reactor.core.publisher.Mono;

public class ServiceTagsExample {

  /**
   * Main runner.
   *
   * @param args arguments
   */
  public static void main(String[] args) {
    Microservices gateway = Microservices.builder().startAwait();

    Microservices services1 =
        Microservices.builder()
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .services(
                ServiceInfo.fromServiceInstance(new GreetingServiceImplA())
                    .tag("Weight", "0.3")
                    .build())
            .startAwait();

    Microservices services2 =
        Microservices.builder()
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .services(
                ServiceInfo.fromServiceInstance(new GreetingServiceImplB())
                    .tag("Weight", "0.7")
                    .build())
            .startAwait();

    CanaryService service =
        gateway.call().router(WeightedRandomRouter.class).create().api(CanaryService.class);

    for (int i = 0; i < 10; i++) {
      Mono.from(service.greeting(new GreetingRequest("joe")))
          .doOnNext(
              success -> {
                success.getResult().startsWith("B");
                System.out.println(success);
              });
    }
  }
}
