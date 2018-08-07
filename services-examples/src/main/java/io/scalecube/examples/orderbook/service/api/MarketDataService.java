package io.scalecube.examples.orderbook.service.api;

import io.scalecube.examples.orderbook.service.OrderBookSnapshoot;
import io.scalecube.examples.orderbook.service.OrderRequest;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service("io.scalecube.examples.MarketData")
public interface MarketDataService {

  @ServiceMethod("processOrder")
  Mono<String> processOrder(OrderRequest order);

  @ServiceMethod("orderBook")
  Flux<OrderBookSnapshoot> orderBook();
}
