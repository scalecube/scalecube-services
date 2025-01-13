package io.scalecube.services.gateway.rest;

import io.scalecube.services.annotations.RestMethod;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

@Service("v1/routingService")
public interface RoutingService {

  @RestMethod("GET")
  @ServiceMethod("find/:foo")
  Mono<SomeResponse> find();

  @RestMethod("POST")
  @ServiceMethod("update/:foo")
  Mono<SomeResponse> update(SomeRequest request);
}
