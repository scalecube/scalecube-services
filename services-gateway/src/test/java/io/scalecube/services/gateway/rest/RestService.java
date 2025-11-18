package io.scalecube.services.gateway.rest;

import io.scalecube.services.annotations.RestMethod;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import reactor.core.publisher.Mono;

@Service("v1/restService")
public interface RestService {

  @ServiceMethod("options/:foo")
  Mono<SomeResponse> options();

  @ServiceMethod("get/:foo")
  Mono<SomeResponse> get();

  @ServiceMethod("head/:foo")
  Mono<SomeResponse> head();

  @ServiceMethod("post/:foo")
  Mono<SomeResponse> post(SomeRequest request);

  @ServiceMethod("put/:foo")
  Mono<SomeResponse> put(SomeRequest request);

  @ServiceMethod("patch/:foo")
  Mono<SomeResponse> patch(SomeRequest request);

  @ServiceMethod("delete/:foo")
  Mono<SomeResponse> delete();

  @ServiceMethod("trace/:foo")
  Mono<SomeResponse> trace();

  @RestMethod("GET")
  @ServiceMethod("propagate/:foo/:bar/:baz")
  Mono<SomeResponse> propagateRequestAttributes();
}
