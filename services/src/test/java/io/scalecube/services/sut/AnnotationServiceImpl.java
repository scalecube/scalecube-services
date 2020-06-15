package io.scalecube.services.sut;

import io.scalecube.services.Microservices;
import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;

public class AnnotationServiceImpl implements AnnotationService {

  private ReplayProcessor<ServiceDiscoveryEvent> serviceDiscoveryEvents;

  @AfterConstruct
  void init(Microservices microservices) {
    this.serviceDiscoveryEvents = ReplayProcessor.create();
    microservices.discovery().listen().subscribe(serviceDiscoveryEvents);
  }

  @Override
  public Flux<ServiceDiscoveryEvent.Type> serviceDiscoveryEventTypes() {
    return serviceDiscoveryEvents.map(ServiceDiscoveryEvent::type);
  }
}
