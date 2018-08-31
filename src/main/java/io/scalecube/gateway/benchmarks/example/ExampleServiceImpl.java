package io.scalecube.gateway.benchmarks.example;

import io.scalecube.gateway.examples.StreamRequest;
import io.scalecube.services.api.ServiceMessage;
import java.time.Duration;
import java.util.stream.LongStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class ExampleServiceImpl implements ExampleService {

  private static final String SERVICE_RECEIVED_TIME_HEADER = "srv-recd-time";

  @Override
  public Mono<String> one(String name) {
    return Mono.just("Echo:" + name);
  }

  @Override
  public Mono<ServiceMessage> oneMessage(ServiceMessage request) {
    return Mono.defer(
        () ->
            Mono.just(
                ServiceMessage.from(request)
                    .header(
                        SERVICE_RECEIVED_TIME_HEADER, String.valueOf(System.currentTimeMillis()))
                    .build()));
  }

  @Override
  public Flux<Long> manyStream(Long cnt) {
    return Flux.fromStream(LongStream.range(0, cnt).boxed())
        .publishOn(Schedulers.parallel(), Integer.MAX_VALUE)
        .onBackpressureDrop();
  }

  @Override
  public Flux<Long> manyStreamWithBackpressureDrop(Long cnt) {
    return Flux.fromStream(LongStream.range(0, cnt).boxed())
        .publishOn(Schedulers.parallel(), Integer.MAX_VALUE)
        .onBackpressureDrop();
  }

  @Override
  public Flux<Long> requestInfiniteStream(StreamRequest request) {
    Flux<Flux<Long>> fluxes =
        Flux.interval(Duration.ofMillis(request.getIntervalMillis()))
            .map(
                tick ->
                    Flux.create(
                        s -> {
                          for (int i = 0; i < request.getMessagesPerInterval(); i++) {
                            s.next(System.currentTimeMillis());
                          }
                          s.complete();
                        }));

    return Flux.concat(fluxes)
        .publishOn(Schedulers.parallel(), Integer.MAX_VALUE)
        .onBackpressureDrop();
  }

  @Override
  public Flux<ServiceMessage> requestInfiniteMessageStream(ServiceMessage request) {
    return Flux.defer(
        () -> {
          Duration interval =
              Duration.ofMillis(Long.parseLong(request.header("executionTaskInterval")));
          int messagesPerInterval =
              Integer.parseInt(request.header("messagesPerExecutionInterval"));

          Flux<Flux<ServiceMessage>> fluxes =
              Flux.interval(interval).map(tick -> emitServiceMessages(messagesPerInterval));

          return Flux.concat(fluxes)
              .publishOn(Schedulers.parallel(), Integer.MAX_VALUE)
              .onBackpressureDrop();
        });
  }

  private Flux<ServiceMessage> emitServiceMessages(int messagesPerInterval) {
    return Flux.create(
        fluxSink -> {
          for (int i = 0; i < messagesPerInterval; i++) {
            fluxSink.next(
                ServiceMessage.builder()
                    .header(
                        SERVICE_RECEIVED_TIME_HEADER, String.valueOf(System.currentTimeMillis()))
                    .build());
          }
          fluxSink.complete();
        });
  }
}
