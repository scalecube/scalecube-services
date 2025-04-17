package io.scalecube.services.gateway;

import static io.scalecube.services.gateway.SecuredService.NAMESPACE;

import io.scalecube.services.annotations.RequestType;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service(NAMESPACE)
public interface SecuredService {

  String NAMESPACE = "gw.auth";

  @ServiceMethod
  @RequestType(String.class)
  Mono<String> createSession(ServiceMessage request);

  @ServiceMethod
  Mono<String> requestOne(String request);

  @ServiceMethod
  Flux<String> requestMany(Integer request);
}
