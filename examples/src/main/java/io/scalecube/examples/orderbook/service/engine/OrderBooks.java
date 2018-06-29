package io.scalecube.examples.orderbook.service.engine;

import io.scalecube.examples.orderbook.service.engine.events.AddOrder;
import io.scalecube.examples.orderbook.service.engine.events.MatchOrder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import reactor.core.publisher.Flux;

public class OrderBooks {

  private Map<String, OrderBook> books = new HashMap<>();
  private Long2ObjectOpenHashMap<Order> orders;

  public OrderBooks(List<String> instruments) {
    this.orders = new Long2ObjectOpenHashMap<>();

    for (String instrument : instruments) {
      OrderBook book = new OrderBook();
      books.put(instrument, book);
    }
  }

  public Flux<MatchOrder> listenMatch(String instrument) {
    return books.get(instrument).matchListener();
  }

  public Flux<AddOrder> listenAdd(String instrument) {
    return books.get(instrument).addListener();
  }

  public OrderBook book(String instrument) {
    return books.get(instrument);
  }

  public void enterOrder(Order order, String instrument) {
    books.get(instrument).enter(order.id(), order.level().side(), order.level().price(), order.size());
  }

  public void cancel(Order order) {
    orders.remove(order.id());
  }



}
