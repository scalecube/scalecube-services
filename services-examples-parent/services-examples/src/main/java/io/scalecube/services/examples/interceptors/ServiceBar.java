package io.scalecube.services.examples.interceptors;

import io.scalecube.services.annotations.RequestType;
import io.scalecube.services.annotations.ResponseType;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import reactor.core.publisher.Mono;

@Service("example.service.bar")
public interface ServiceBar {

  @RequestType(Integer.class)
  @ResponseType(Integer.class)
  @ServiceMethod
  Mono<ServiceMessage> bar(ServiceMessage input);
}
