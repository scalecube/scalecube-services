package io.scalecube.services.methods;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.scalecube.services.auth.Principal;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class StubServiceImpl implements StubService {

  @Override
  public Mono<String> returnNull() {
    return null;
  }

  @Override
  public Flux<String> returnNull2() {
    return null;
  }

  @Override
  public Flux<String> returnNull3(Flux<String> request) {
    return null;
  }

  @Override
  public Mono<String> throwException() {
    throw new RuntimeException();
  }

  @Override
  public Flux<String> throwException2() {
    throw new RuntimeException();
  }

  @Override
  public Flux<String> throwException3(Flux<String> request) {
    throw new RuntimeException();
  }

  @Override
  public Mono<Void> helloAuthContext() {
    return Principal.deferSecured(StubServicePrincipal.class).then();
  }

  @Override
  public Mono<Void> helloRequestContextWithDynamicQualifier() {
    return RequestContext.deferContextual()
        .doOnNext(
            requestContext -> {
              assertNotNull(requestContext.headers(), "requestContext.headers");
              assertNotNull(requestContext.principal(), "requestContext.principal");
              assertNotNull(requestContext.pathVars(), "requestContext.pathVars");
              assertNotNull(requestContext.pathVar("foo"), "foo");
              assertNotNull(requestContext.pathVar("bar"), "bar");
            })
        .then();
  }
}
