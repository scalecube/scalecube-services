package io.scalecube.services;

import static reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping;

import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.discovery.api.ServiceDiscoveryFactory;
import io.scalecube.services.discovery.api.ServiceDiscoveryOptions;
import java.time.Duration;
import java.util.function.UnaryOperator;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class ServiceDiscoveryBootstrap implements AutoCloseable {

  private ServiceDiscovery serviceDiscovery;
  private final Sinks.Many<ServiceDiscoveryEvent> sink =
      Sinks.many().multicast().directBestEffort();
  private final Disposable.Composite disposables = Disposables.composite();
  private Scheduler scheduler;

  ServiceDiscoveryBootstrap() {}

  private ServiceDiscoveryBootstrap conclude(
      Microservices microservices, ServiceDiscoveryOptions options) {
    this.microservices = microservices;
    this.scheduler = Schedulers.newSingle("discovery", true);

    if (operator == null) {
      return this;
    }

    options = operator.apply(options);
    final ServiceEndpoint serviceEndpoint = options.serviceEndpoint();
    final ServiceDiscoveryFactory discoveryFactory = options.discoveryFactory();

    if (discoveryFactory == null) {
      return this;
    }

    serviceDiscovery = discoveryFactory.createServiceDiscovery(serviceEndpoint);

    return this;
  }

  private Mono<Void> startListen() {
    return Mono.defer(
        () -> {
          if (serviceDiscovery == null) {
            return Mono.empty();
          }

          disposables.add(
              serviceDiscovery
                  .listen()
                  .subscribeOn(scheduler)
                  .publishOn(scheduler)
                  .doOnNext(event -> onDiscoveryEvent(microservices, event))
                  .doOnNext(event -> sink.emitNext(event, busyLooping(Duration.ofSeconds(3))))
                  .subscribe());

          return Mono.fromRunnable(serviceDiscovery::start)
              .then()
              .doOnSubscribe(s -> LOGGER.info("[{}][startListen] Starting", microservices.id()))
              .doOnSuccess(avoid -> LOGGER.info("[{}][startListen] Started", microservices.id()))
              .doOnError(
                  ex ->
                      LOGGER.error(
                          "[{}][startListen] Exception occurred: {}",
                          microservices.id(),
                          ex.toString()));
        });
  }

  public Flux<ServiceDiscoveryEvent> listen() {
    return Flux.fromStream(microservices.serviceRegistry.listServiceEndpoints().stream())
        .map(ServiceDiscoveryEvent::newEndpointAdded)
        .concatWith(sink.asFlux().onBackpressureBuffer())
        .subscribeOn(scheduler)
        .publishOn(scheduler);
  }

  private void onDiscoveryEvent(Microservices microservices, ServiceDiscoveryEvent event) {
    if (event.isEndpointAdded()) {
      microservices.serviceRegistry.registerService(event.serviceEndpoint());
    }
    if (event.isEndpointLeaving() || event.isEndpointRemoved()) {
      microservices.serviceRegistry.unregisterService(event.serviceEndpoint().id());
    }
  }

  @Override
  public void close() {
    disposables.dispose();

    sink.emitComplete(busyLooping(Duration.ofSeconds(3)));

    try {
      if (serviceDiscovery != null) {
        serviceDiscovery.shutdown();
      }
    } finally {
      if (scheduler != null) {
        scheduler.dispose();
      }
    }
  }
}
