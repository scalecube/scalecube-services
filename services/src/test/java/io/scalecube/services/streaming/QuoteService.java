package io.scalecube.services.streaming;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service(QuoteService.NAME)
public interface QuoteService {

  String NAME = "io.sc.quote-service";

  @ServiceMethod
  Flux<String> quotes(int maxSize);

  @ServiceMethod
  Flux<String> snapshoot(int size);

  @ServiceMethod
  Mono<String> justOne();

  @ServiceMethod
  Flux<String> scheduled(int interval);
  
}
