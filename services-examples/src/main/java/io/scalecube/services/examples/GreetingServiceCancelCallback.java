package io.scalecube.services.examples;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class GreetingServiceCancelCallback implements GreetingService {

  private final GreetingService greetingService = new GreetingServiceImpl();
  private final Runnable onCancel;

  public GreetingServiceCancelCallback(Runnable onCancel) {
    this.onCancel = onCancel;
  }

  @Override
  public Mono<String> one(String name) {
    if (name == null) {
      throw new BadRequestException("Wrong request");
    }
    return greetingService.one(name).doOnCancel(onCancel);
  }

  @Override
  public Flux<Long> manyStream(Long cnt) {
    return greetingService.manyStream(cnt).doOnCancel(onCancel);
  }

  @Override
  public Mono<String> failingOne(String name) {
    return greetingService.failingOne(name).doOnCancel(onCancel);
  }

  @Override
  public Flux<String> many(String name) {
    return greetingService.many(name).doOnCancel(onCancel);
  }

  @Override
  public Flux<String> failingMany(String name) {
    return greetingService.failingMany(name).doOnCancel(onCancel);
  }

  @Override
  public Mono<GreetingResponse> pojoOne(GreetingRequest request) {
    return greetingService.pojoOne(request).doOnCancel(onCancel);
  }

  @Override
  public Mono<List<GreetingResponse>> pojoList(GreetingRequest request) {
    return greetingService.pojoList(request).doOnCancel(onCancel);
  }

  @Override
  public Flux<GreetingResponse> pojoMany(GreetingRequest request) {
    return greetingService.pojoMany(request).doOnCancel(onCancel);
  }

  @Override
  public Mono<String> emptyOne(String name) {
    return greetingService.emptyOne(name).doOnCancel(onCancel);
  }

  @Override
  public Flux<String> emptyMany(String name) {
    return greetingService.emptyMany(name).doOnCancel(onCancel);
  }

  @Override
  public Mono<String> neverOne(String name) {
    return greetingService.neverOne(name).doOnCancel(onCancel);
  }

  @Override
  public Mono<String> delayOne(String name) {
    return greetingService.delayOne(name).doOnCancel(onCancel);
  }

  @Override
  public Flux<String> delayMany(String name) {
    return greetingService.delayMany(name).doOnCancel(onCancel);
  }

  @Override
  public Mono<EmptyGreetingResponse> emptyGreeting(EmptyGreetingRequest request) {
    return greetingService.emptyGreeting(request).doOnCancel(onCancel);
  }

  @Override
  public Mono<ServiceMessage> emptyGreetingMessage(ServiceMessage request) {
    return greetingService.emptyGreetingMessage(request).doOnCancel(onCancel);
  }

  @Override
  public Mono<String> helloDynamicQualifier(Long value) {
    return greetingService.helloDynamicQualifier(value).doOnCancel(onCancel);
  }
}
