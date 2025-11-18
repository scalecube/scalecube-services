package io.scalecube.services.examples;

import io.scalecube.services.RequestContext;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class GreetingServiceImpl implements GreetingService {

  @Override
  public Mono<String> one(String name) {
    if (name == null) {
      throw new BadRequestException("Wrong request");
    }
    return Mono.defer(() -> Mono.just("Echo:" + name));
  }

  @Override
  public Flux<Long> manyStream(Long cnt) {
    return Flux.defer(
        () -> Flux.fromStream(LongStream.range(0, cnt).boxed()).publishOn(Schedulers.parallel()));
  }

  @Override
  public Mono<String> failingOne(String name) {
    return Mono.defer(() -> Mono.error(new RuntimeException(name)));
  }

  @Override
  public Flux<String> many(String name) {
    return Flux.defer(
        () -> Flux.interval(Duration.ofMillis(100)).map(i -> "Greeting (" + i + ") to: " + name));
  }

  @Override
  public Flux<String> failingMany(String name) {
    return Flux.defer(
        () ->
            Flux.push(
                sink -> {
                  sink.next("Echo:" + name);
                  sink.next("Echo:" + name);
                  sink.error(new RuntimeException("Echo:" + name));
                }));
  }

  @Override
  public Mono<GreetingResponse> pojoOne(GreetingRequest request) {
    return Mono.defer(() -> one(request.getText()).map(GreetingResponse::new));
  }

  @Override
  public Mono<List<GreetingResponse>> pojoList(GreetingRequest request) {
    return Mono.defer(
        () -> one(request.getText()).map(GreetingResponse::new).map(Collections::singletonList));
  }

  @Override
  public Flux<GreetingResponse> pojoMany(GreetingRequest request) {
    return Flux.defer(() -> many(request.getText()).map(GreetingResponse::new));
  }

  @Override
  public Mono<String> emptyOne(String name) {
    return Mono.empty();
  }

  @Override
  public Flux<String> emptyMany(String name) {
    return Flux.empty();
  }

  @Override
  public Mono<String> neverOne(String name) {
    return Mono.never();
  }

  @Override
  public Mono<String> delayOne(String name) {
    return Mono.defer(() -> Mono.delay(Duration.ofSeconds(1)).then(Mono.just(name)));
  }

  @Override
  public Flux<String> delayMany(String name) {
    return Flux.defer(
        () -> Flux.interval(Duration.ofMillis(500), Duration.ofSeconds(2)).map(i -> name));
  }

  @Override
  public Mono<EmptyGreetingResponse> emptyGreeting(EmptyGreetingRequest request) {
    return Mono.just(new EmptyGreetingResponse());
  }

  @Override
  public Mono<ServiceMessage> emptyGreetingMessage(ServiceMessage request) {
    ServiceMessage response =
        ServiceMessage.from(request).data(new EmptyGreetingResponse()).build();
    return Mono.just(response);
  }

  @Override
  public Mono<String> helloDynamicQualifier(Long value) {
    return RequestContext.deferContextual()
        .map(context -> context.pathParams().stringValue("someVar") + "@" + value);
  }
}
