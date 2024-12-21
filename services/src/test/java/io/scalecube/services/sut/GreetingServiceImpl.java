package io.scalecube.services.sut;

import io.scalecube.services.Microservices;
import io.scalecube.services.annotations.Inject;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.methods.RequestContext;
import java.util.stream.LongStream;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public final class GreetingServiceImpl implements GreetingService {

  @Inject Microservices ms;

  private int instanceId;

  public GreetingServiceImpl() {}

  public GreetingServiceImpl(int id) {
    this.instanceId = id;
  }

  @Override
  public String toString() {
    return "GreetingServiceImpl []";
  }

  @Override
  public Mono<String> greeting(String name) {
    return Mono.just(" hello to: " + name);
  }

  @Override
  public Mono<GreetingResponse> greetingPojo(GreetingRequest name) {
    return Mono.just(new GreetingResponse(" hello to: " + name));
  }

  @Override
  public Mono<GreetingResponse> greetingNotAuthorized(GreetingRequest name) {
    return Mono.error(new ForbiddenException("Not authorized"));
  }

  @Override
  public Mono<GreetingResponse> greetingRequestTimeout(GreetingRequest request) {
    System.out.println(
        "[greetingRequestTimeout] Hello... i am a service an just recived a message:" + request);
    return Mono.delay(request.getDuration())
        .flatMap(
            i ->
                Mono.just(
                    new GreetingResponse(
                        " hello to: " + request.getName(), String.valueOf(this.hashCode()))));
  }

  @Override
  public Mono<String> greetingNoParams() {
    System.out.println(
        "[greetingNoParams] Hello... i am a service an just recived "
            + "a call bu i dont know from who.");
    return Mono.just("hello unknown");
  }

  @Override
  public Mono<GreetingResponse> greetingRequest(GreetingRequest request) {
    System.out.println(
        instanceId
            + ":[greetingRequest] Hello... i am a service an just recived a message:"
            + request);
    return Mono.just(new GreetingResponse(" hello to: " + request.getName(), "" + instanceId));
  }

  @Override
  public Flux<GreetingResponse> bidiGreeting(Publisher<GreetingRequest> request) {
    return Flux.from(request)
        .map(onNext -> new GreetingResponse(" hello to: " + onNext.getName(), "" + instanceId));
  }

  @Override
  public Flux<ServiceMessage> bidiGreetingMessage(Publisher<ServiceMessage> requests) {
    return Flux.from(requests)
        .map(
            request -> {
              GreetingRequest data = request.data();
              GreetingResponse resp = new GreetingResponse(" hello to: " + data.getName(), "1");
              return ServiceMessage.builder().data(resp).build();
            });
  }

  @Override
  public Flux<GreetingResponse> bidiGreetingNotAuthorized(Flux<GreetingRequest> request) {
    return Flux.error(new ForbiddenException("Not authorized"));
  }

  @Override
  public Flux<ServiceMessage> bidiGreetingNotAuthorizedMessage(Publisher<ServiceMessage> requests) {
    return Flux.error(new ForbiddenException("Not authorized"));
  }

  @Override
  public Flux<GreetingResponse> bidiGreetingIllegalArgumentException(
      Publisher<GreetingRequest> request) {
    throw new IllegalArgumentException("IllegalArgumentException");
  }

  @Override
  public Flux<ServiceMessage> bidiGreetingIllegalArgumentExceptionMessage(
      Publisher<ServiceMessage> requests) {
    throw new IllegalArgumentException("IllegalArgumentException");
  }

  @Override
  public Mono<GreetingResponse> greetingMonoEmpty(GreetingRequest request) {
    return Mono.empty();
  }

  @Override
  public Flux<GreetingResponse> greetingFluxEmpty(GreetingRequest request) {
    return Flux.empty();
  }

  @Override
  public Mono<ServiceMessage> greetingMessage(ServiceMessage request) {
    System.out.println(
        "[greetingMessage] Hello... i am a service an just recived a message:" + request);
    GreetingRequest data = request.data();
    GreetingResponse resp = new GreetingResponse("hello to: " + data.getName(), "1");
    return Mono.just(ServiceMessage.builder().data(resp).build());
  }

  @Override
  public Mono<GreetingResponse> greetingMessage2(ServiceMessage request) {
    System.out.println(
        "[greetingMessage] Hello... i am a service an just recived a message:" + request);
    GreetingRequest data = request.data();
    GreetingResponse resp = new GreetingResponse("hello to: " + data.getName(), "1");
    return Mono.just(resp);
  }

  @Override
  public Mono<Void> greetingVoid(GreetingRequest request) {
    System.out.println(
        "[greetingVoid] Hello... i am a service an just recived a message:" + request);
    System.out.println(" hello to: " + request.getName());
    return Mono.empty();
  }

  @Override
  public Mono<Void> failingVoid(GreetingRequest request) {
    System.out.println(
        "[failingVoid] Hello... i am a service an just recived a message:" + request);
    return Mono.error(new IllegalArgumentException(request.toString()));
  }

  @Override
  public Mono<Void> throwingVoid(GreetingRequest request) {
    System.out.println(
        "[failingVoid] Hello... i am a service an just recived a message:" + request);
    throw new IllegalArgumentException(request.toString());
  }

  @Override
  public Mono<GreetingResponse> failingRequest(GreetingRequest request) {
    System.out.println(
        "[failingRequest] Hello... i am a service an just recived a message:" + request);
    return Mono.error(new IllegalArgumentException(request.toString()));
  }

  @Override
  public Mono<GreetingResponse> exceptionRequest(GreetingRequest request) {
    System.out.println(
        "[exceptionRequest] Hello... i am a service an just recived a message:" + request);
    throw new IllegalArgumentException(request.toString());
  }

  @Override
  public Mono<EmptyGreetingResponse> emptyGreeting(EmptyGreetingRequest request) {
    System.out.println("[emptyGreeting] service received a message:" + request);
    return Mono.just(new EmptyGreetingResponse());
  }

  @Override
  public Mono<ServiceMessage> emptyGreetingMessage(ServiceMessage request) {
    System.out.println("[emptyGreetingMessage] service received a message:" + request);
    return Mono.just(ServiceMessage.from(request).data(new EmptyGreetingResponse()).build());
  }

  @Override
  public void notifyGreeting() {
    System.out.println("[notifyGreeting] Hello... i am a service and i just notefied");
  }

  @Override
  public Flux<Long> manyStream(Long cnt) {
    return Flux.defer(
        () -> Flux.fromStream(LongStream.range(0, cnt).boxed()).publishOn(Schedulers.parallel()));
  }

  @Override
  public Mono<String> helloDynamicQualifier(Long value) {
    return RequestContext.deferContextual()
        .map(context -> context.pathVar("someVar") + "@" + value);
  }
}
