package io.scalecube.services;

import java.util.Collection;
import java.util.Collections;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServicesLifeCycleManager {

  ServicesLifeCycleManager EMPTY =
      new ServicesLifeCycleManager() {
        @Override
        public Mono<? extends Collection<ServiceInfo>> constructServices(Microservices microservices) {
          return Mono.just(Collections.emptyList());
        }

        @Override
        public Mono<Void> postConstruct(Microservices microservices) {
          return Mono.empty();
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
  static ServicesLifeCycleManager union(
      ServicesLifeCycleManager provider1, ServicesLifeCycleManager provider2) {
    return new ServicesLifeCycleManager() {
      @Override
      public Mono<? extends Collection<ServiceInfo>> constructServices(Microservices microservices) {
        Flux<ServiceInfo> services1 = flatService(microservices, provider1);
        Flux<ServiceInfo> services2 = flatService(microservices, provider2);
        return services1.mergeWith(services2).collectList();
      }

      @Override
      public Mono<Void> postConstruct(Microservices microservices) {
        Mono<Void> void1 = provider1.postConstruct(microservices);
        Mono<Void> void2 = provider2.postConstruct(microservices);
        return void1.then(void2);
      }

      @Override
      public Mono<Microservices> shutDown(Microservices microservices) {
        return provider1.shutDown(microservices).then(provider2.shutDown(microservices));
      }

      private Flux<ServiceInfo> flatService(
          Microservices microservices, ServicesLifeCycleManager provider) {
        return provider.constructServices(microservices).flatMapIterable(i -> i);
      }
    };
  }

  Mono<? extends Collection<ServiceInfo>> constructServices(Microservices microservices);

  Mono<Void> postConstruct(Microservices microservices);

  Mono<Microservices> shutDown(Microservices microservices);
}
