package io.scalecube.services.examples.orderbook.service;

import io.scalecube.services.examples.orderbook.service.engine.events.Side;

/** The interface for outbound events from a market. */
public interface MarketListener {

  /**
   * An event indicating that an order book has changed.
   *
   * @param book the order book
   * @param bbo true if the best bid and offer (BBO) has changed, otherwise false
   */
  void update(OrderBook book, boolean bbo);

  /**
   * An event indicating that a trade has taken place.
   *
   * @param book the order book
   * @param side the side of the resting order
   * @param price the trade price
   * @param size the trade size
   */
  void trade(OrderBook book, Side side, long price, long size);
}
