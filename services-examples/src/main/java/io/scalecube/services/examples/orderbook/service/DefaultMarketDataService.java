package io.scalecube.services.examples.orderbook.service;

import io.scalecube.services.examples.orderbook.service.api.MarketDataService;
import io.scalecube.services.examples.orderbook.service.engine.OrderBooks;
import io.scalecube.services.examples.orderbook.service.engine.events.Side;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultMarketDataService implements MarketDataService {

  private OrderBooks books;
  private AtomicLong lastTrade = new AtomicLong();
  private Map<Long, OrderBook> view = new ConcurrentHashMap<>();
  private Market market = new Market(new MyMarketListener());

  /** Create a new service. */
  public DefaultMarketDataService() {
    List<String> instumentList = new ArrayList<>();
    instumentList.add("ORCL");
    books = new OrderBooks(instumentList);
    OrderBook marketBook = market.open(1L);
    view.put(1L, marketBook);
    books
        .listenAdd("ORCL")
        .subscribe(add -> market.execute(add.orderId(), add.quantity(), add.price()));

    books
        .listenMatch("ORCL")
        .subscribe(
            match -> {
              if (match.remainingQuantity() == 0) {
                market.delete(match.incomingOrderId());
              } else {
                market.execute(match.incomingOrderId(), match.executedQuantity(), match.price());
              }
            });
  }

  @Override
  public Mono<String> processOrder(OrderRequest request) {

    books.enterOrder(request.order(), request.instrument());
    market.add(
        1L,
        request.order().id(),
        request.order().level().side(),
        request.order().level().price(),
        request.order().size());
    return Mono.just("OK");
  }

  @Override
  public Flux<OrderBookSnapshoot> orderBook() {

    return Flux.interval(Duration.ofSeconds(1))
        .map(mapper -> new OrderBookSnapshoot(view.get(1L), lastTrade.get()));
  }

  private static class MyMarketListener implements MarketListener {

    @Override
    public void update(OrderBook book, boolean bbo) {
      System.out.println(book);
    }

    @Override
    public void trade(OrderBook book, Side side, long price, long size) {
      System.out.println(book);
    }
  }
}
