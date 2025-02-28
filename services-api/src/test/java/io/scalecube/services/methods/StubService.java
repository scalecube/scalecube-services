package io.scalecube.services.methods;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.auth.Secured;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service(StubService.NAMESPACE)
public interface StubService {

  String NAMESPACE = "v1/stubService";

  // Invocation methods

  @ServiceMethod
  Mono<String> invokeOneReturnsNull();

  @ServiceMethod
  Flux<String> invokeManyReturnsNull();

  @ServiceMethod
  Flux<String> invokeBidirectionalReturnsNull(Flux<String> request);

  @ServiceMethod
  Mono<String> invokeOneThrowsException();

  @ServiceMethod
  Flux<String> invokeManyThrowsException();

  @ServiceMethod
  Flux<String> invokeBidirectionalThrowsException(Flux<String> request);

  @ServiceMethod("hello/:foo/dynamic/:bar")
  Mono<Void> invokeDynamicQualifier();

  // Secured methods

  @Secured
  @ServiceMethod
  Mono<Void> invokeWithAuthContext();

  // Services secured by code in method body

  @Secured
  @ServiceMethod
  Mono<Void> invokeWithRoleOrPermissions();
}
