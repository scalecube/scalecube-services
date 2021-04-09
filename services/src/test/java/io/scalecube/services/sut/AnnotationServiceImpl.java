package io.scalecube.services.sut;

import io.scalecube.services.Microservices;
import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public class AnnotationServiceImpl implements AnnotationService {

  private Sinks.Many<ServiceDiscoveryEvent> serviceDiscoveryEvents;

  @AfterConstruct
  void init(Microservices microservices) {
    this.serviceDiscoveryEvents = Sinks.many().replay().all();
    microservices
        .listenDiscovery()
        .subscribe(
            serviceDiscoveryEvents::tryEmitNext,
            serviceDiscoveryEvents::tryEmitError,
            serviceDiscoveryEvents::tryEmitComplete);
  }

  @Override
  public Flux<ServiceDiscoveryEvent.Type> serviceDiscoveryEventTypes() {
    return serviceDiscoveryEvents.asFlux().onBackpressureBuffer().map(ServiceDiscoveryEvent::type);
  }
}
