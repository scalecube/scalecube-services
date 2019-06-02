package io.scalecube.services.examples.orderbook.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OrderBookSnapshoot {

  Map<Long, Long> bids = new HashMap<Long, Long>();

  Map<Long, Long> asks = new HashMap<Long, Long>();

  Long currentPrice;

  /**
   * Create a new snapshot of the orderbook.
   *
   * @param orderBook an order book to take snapshot from.
   * @param currentPrice the current price.
   */
  public OrderBookSnapshoot(OrderBook orderBook, Long currentPrice) {
    Set<Long> askPrices = Collections.unmodifiableSet(orderBook.getAskPrices());
    Set<Long> bidPrices = Collections.unmodifiableSet(orderBook.getBidPrices());
    askPrices.forEach(price -> asks.put(price, orderBook.getAskSize(price)));
    bidPrices.forEach(price -> bids.put(price, orderBook.getBidSize(price)));
    this.currentPrice = currentPrice;
  }

  public Map<Long, Long> bids() {
    return bids;
  }

  public Map<Long, Long> asks() {
    return asks;
  }

  public Long currentPrice() {
    return currentPrice;
  }
}
