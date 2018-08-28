package io.scalecube.gateway.benchmarks.example;

import static io.scalecube.gateway.examples.GreetingService.TIMESTAMP_KEY;

import io.scalecube.gateway.examples.StreamRequest;
import io.scalecube.services.api.ServiceMessage;
import java.time.Duration;
import java.util.stream.LongStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class ExampleServiceImpl implements ExampleService {

  private Flux<Integer> source =
      Flux.just(1)
          .repeat()
          .subscribeOn(Schedulers.newParallel("service-source"))
          .publish()
          .autoConnect();

  @Override
  public Mono<String> one(String name) {
    return Mono.just("Echo:" + name);
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
  public Flux<Long> broadcastStream() {
    return source.map(i -> System.currentTimeMillis());
  }

  @Override
  public Flux<ServiceMessage> rawBroadcastStream() {
    return source.map(
        i ->
            ServiceMessage.builder()
                .header(TIMESTAMP_KEY, Long.toString(System.currentTimeMillis()))
                .build());
  }
}
