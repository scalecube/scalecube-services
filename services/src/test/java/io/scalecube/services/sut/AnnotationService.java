package io.scalecube.services.sut;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import reactor.core.publisher.Flux;

@Service(AnnotationService.SERVICE_NAME)
public interface AnnotationService {

  String SERVICE_NAME = "annotations";

  @ServiceMethod
  Flux<ServiceDiscoveryEvent.Type> serviceDiscoveryEventTypes();
}
