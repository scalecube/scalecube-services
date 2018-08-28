package io.scalecube.examples.orderbook.service;

import io.scalecube.examples.orderbook.service.engine.events.Side;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/** A market. */
public class Market {
  private Long2ObjectOpenHashMap<OrderBook> books;

  private Long2ObjectOpenHashMap<Order> orders;

  private MarketListener listener;

  /**
   * Create a market.
   *
   * @param listener a listener for outbound events from the market
   */
  public Market(MarketListener listener) {
    this.books = new Long2ObjectOpenHashMap<>();
    this.orders = new Long2ObjectOpenHashMap<>();

    this.listener = listener;
  }

  /**
   * Open an order book.
   *
   * <p>If the order book for the instrument is already open, do nothing.
   *
   * @param instrument an instrument
   * @return the order book
   */
  public OrderBook open(long instrument) {
    return books.computeIfAbsent(instrument, (key) -> new OrderBook(key));
  }

  /**
   * Find an order.
   *
   * @param orderId the order identifier
   * @return the order or {@code null} if the order identifier is unknown
   */
  public Order find(long orderId) {
    return orders.get(orderId);
  }

  /**
   * Add an order to an order book.
   *
   * <p>An update event is triggered.
   *
   * <p>If the order book for the instrument is closed or the order identifier is known, do nothing.
   *
   * @param instrument the instrument
   * @param orderId the order identifier
   * @param side the side
   * @param price the price
   * @param size the size
   */
  public void add(long instrument, long orderId, Side side, long price, long size) {
    if (orders.containsKey(orderId)) {
      return;
    }

    OrderBook book = books.get(instrument);
    if (book == null) {
      return;
    }

    Order order = new Order(book, side, price, size);

    boolean bbo = book.add(side, price, size);

    orders.put(orderId, order);

    listener.update(book, bbo);
  }

  /**
   * Modify an order in an order book. The order will retain its time priority. If the new size is
   * zero, the order is deleted from the order book.
   *
   * <p>An update event is triggered.
   *
   * <p>If the order identifier is unknown, do nothing.
   *
   * @param orderId the order identifier
   * @param size the new size
   */
  public void modify(long orderId, long size) {
    Order order = orders.get(orderId);
    if (order == null) {
      return;
    }

    OrderBook book = order.getOrderBook();

    long newSize = Math.max(0, size);

    boolean bbo =
        book.update(order.getSide(), order.getPrice(), newSize - order.getRemainingQuantity());

    if (newSize == 0) {
      orders.remove(orderId);
    } else {
      order.setRemainingQuantity(newSize);
    }

    listener.update(book, bbo);
  }

  /**
   * Execute a quantity of an order in an order book. If the remaining quantity reaches zero, the
   * order is deleted from the order book.
   *
   * <p>A Trade event and an update event are triggered.
   *
   * <p>If the order identifier is unknown, do nothing.
   *
   * @param orderId the order identifier
   * @param quantity the executed quantity
   * @return the remaining quantity
   */
  public long execute(long orderId, long quantity) {
    Order order = orders.get(orderId);
    if (order == null) {
      return 0;
    }

    return execute(orderId, order, quantity, order.getPrice());
  }

  /**
   * Execute a quantity of an order in an order book. If the remaining quantity reaches zero, the
   * order is deleted from the order book.
   *
   * <p>A Trade event and an update event are triggered.
   *
   * <p>If the order identifier is unknown, do nothing.
   *
   * @param orderId the order identifier
   * @param quantity the executed quantity
   * @param price the execution price
   * @return the remaining quantity
   */
  public long execute(long orderId, long quantity, long price) {
    Order order = orders.get(orderId);
    if (order == null) {
      return 0;
    }

    return execute(orderId, order, quantity, price);
  }

  private long execute(long orderId, Order order, long quantity, long price) {
    OrderBook book = order.getOrderBook();

    Side side = order.getSide();

    long remainingQuantity = order.getRemainingQuantity();

    long executedQuantity = Math.min(quantity, remainingQuantity);

    listener.trade(book, side, price, executedQuantity);

    book.update(side, order.getPrice(), -executedQuantity);

    if (executedQuantity == remainingQuantity) {
      orders.remove(orderId);
    } else {
      order.reduce(executedQuantity);
    }

    listener.update(book, true);

    return remainingQuantity - executedQuantity;
  }

  /**
   * Cancel a quantity of an order in an order book. If the remaining quantity reaches zero, the
   * order is deleted from the order book.
   *
   * <p>An update event is triggered.
   *
   * <p>If the order identifier is unknown, do nothing.
   *
   * @param orderId the order identifier
   * @param quantity the canceled quantity
   * @return the remaining quantity
   */
  public long cancel(long orderId, long quantity) {
    Order order = orders.get(orderId);
    if (order == null) {
      return 0;
    }

    OrderBook book = order.getOrderBook();

    long remainingQuantity = order.getRemainingQuantity();

    long canceledQuantity = Math.min(quantity, remainingQuantity);

    boolean bbo = book.update(order.getSide(), order.getPrice(), -canceledQuantity);

    if (canceledQuantity == remainingQuantity) {
      orders.remove(orderId);
    } else {
      order.reduce(canceledQuantity);
    }

    listener.update(book, bbo);

    return remainingQuantity - canceledQuantity;
  }

  /**
   * Delete an order from an order book.
   *
   * <p>An update event is triggered.
   *
   * <p>If the order identifier is unknown, do nothing.
   *
   * @param orderId the order identifier
   */
  public void delete(long orderId) {
    Order order = orders.get(orderId);
    if (order == null) {
      return;
    }

    OrderBook book = order.getOrderBook();

    boolean bbo = book.update(order.getSide(), order.getPrice(), -order.getRemainingQuantity());

    orders.remove(orderId);

    listener.update(book, bbo);
  }
}
