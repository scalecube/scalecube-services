package io.scalecube.services;

import java.util.Collection;
import java.util.Collections;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServicesProvider {

  ServicesProvider EMPTY =
      new ServicesProvider() {
        @Override
        public Mono<? extends Collection<ServiceInfo>> provide(Microservices microservices) {
          return Mono.just(Collections.emptyList());
        }

        @Override
        public Mono<Microservices> shutDown(Microservices microservices) {
          return Mono.just(microservices);
        }
      };

  /**
   * Union of two services providers.
   *
   * @param provider1 first provider
   * @param provider2 second provider
   * @return provider
   */
  static ServicesProvider union(ServicesProvider provider1, ServicesProvider provider2) {
    return new ServicesProvider() {
      @Override
      public Mono<? extends Collection<ServiceInfo>> provide(Microservices microservices) {
        Flux<ServiceInfo> services1 = flatService(microservices, provider1);
        Flux<ServiceInfo> services2 = flatService(microservices, provider2);
        return services1.mergeWith(services2).collectList();
      }

      @Override
      public Mono<Microservices> shutDown(Microservices microservices) {
        return provider1.shutDown(microservices).then(provider2.shutDown(microservices));
      }

      private Flux<ServiceInfo> flatService(
          Microservices microservices, ServicesProvider provider) {
        return provider.provide(microservices).flatMapIterable(i -> i);
      }
    };
  }

  Mono<? extends Collection<ServiceInfo>> provide(Microservices microservices);

  Mono<Microservices> shutDown(Microservices microservices);
}
