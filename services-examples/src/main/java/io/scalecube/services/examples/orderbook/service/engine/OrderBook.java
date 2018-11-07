package io.scalecube.services.examples.orderbook.service.engine;

import io.scalecube.services.examples.orderbook.service.engine.events.AddOrder;
import io.scalecube.services.examples.orderbook.service.engine.events.CancelOrder;
import io.scalecube.services.examples.orderbook.service.engine.events.MatchOrder;
import io.scalecube.services.examples.orderbook.service.engine.events.Side;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.LongComparators;
import java.util.Set;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;

/** An order book. */
public class OrderBook {

  private String instrument;

  private Long2ObjectRBTreeMap<PriceLevel> bids;
  private Long2ObjectRBTreeMap<PriceLevel> asks;

  private Long2ObjectOpenHashMap<Order> orders;

  EmitterProcessor<MatchOrder> matchListener = EmitterProcessor.<MatchOrder>create();
  EmitterProcessor<AddOrder> addListener = EmitterProcessor.<AddOrder>create();
  EmitterProcessor<CancelOrder> cancelListener = EmitterProcessor.<CancelOrder>create();

  /** Create an order book. */
  public OrderBook() {
    this.bids = new Long2ObjectRBTreeMap<>(LongComparators.OPPOSITE_COMPARATOR);
    this.asks = new Long2ObjectRBTreeMap<>(LongComparators.NATURAL_COMPARATOR);
    this.orders = new Long2ObjectOpenHashMap<>();
  }

  /**
   * Enter an order to this order book.
   *
   * <p>The incoming order is first matched against resting orders in this order book. This
   * operation results in zero or more Match events.
   *
   * <p>If the remaining quantity is not zero after the matching operation, the remaining quantity
   * is added to this order book and an Add event is triggered.
   *
   * <p>If the order identifier is known, do nothing.
   *
   * @param orderId an order identifier
   * @param side the side
   * @param price the limit price
   * @param size the size
   */
  public void enter(long orderId, Side side, long price, long size) {
    if (orders.containsKey(orderId)) {
      return;
    }

    if (side == Side.BUY) {
      buy(orderId, price, size);
    } else {
      sell(orderId, price, size);
    }
  }

  private void buy(long orderId, long price, long size) {
    long remainingQuantity = size;

    PriceLevel bestLevel = getBestLevel(asks);

    while (remainingQuantity > 0 && bestLevel != null && bestLevel.price() <= price) {
      remainingQuantity = bestLevel.match(orderId, Side.BUY, remainingQuantity, matchListener);

      if (bestLevel.isEmpty()) {
        asks.remove(bestLevel.price());
      }

      bestLevel = getBestLevel(asks);
    }

    if (remainingQuantity > 0) {
      orders.put(orderId, add(bids, orderId, Side.BUY, price, remainingQuantity));
      addListener.onNext(new AddOrder(orderId, Side.BUY, price, remainingQuantity));
    }
  }

  private void sell(long orderId, long price, long size) {
    long remainingQuantity = size;
    PriceLevel bestLevel = getBestLevel(bids);
    while (remainingQuantity > 0 && bestLevel != null && bestLevel.price() >= price) {
      remainingQuantity = bestLevel.match(orderId, Side.SELL, remainingQuantity, matchListener);
      if (bestLevel.isEmpty()) {
        bids.remove(bestLevel.price());
      }
      bestLevel = getBestLevel(bids);
    }

    if (remainingQuantity > 0) {
      orders.put(orderId, add(asks, orderId, Side.SELL, price, remainingQuantity));
      addListener.onNext(new AddOrder(orderId, Side.SELL, price, remainingQuantity));
    }
  }

  /**
   * Cancel a quantity of an order in this order book. The size refers to the new order size. If the
   * new order size is set to zero, the order is deleted from this order book.
   *
   * <p>A Cancel event is triggered.
   *
   * <p>If the order identifier is unknown, do nothing.
   *
   * @param orderId the order identifier
   * @param size the new size
   */
  public void cancel(long orderId, long size) {
    Order order = orders.get(orderId);
    if (order == null) {
      return;
    }

    long remainingQuantity = order.size();

    if (size >= remainingQuantity) {
      return;
    }

    if (size > 0) {
      order.resize(size);
    } else {
      delete(order);
      orders.remove(orderId);
    }

    cancelListener.onNext(new CancelOrder(orderId, remainingQuantity - size, size));
  }

  private PriceLevel getBestLevel(Long2ObjectRBTreeMap<PriceLevel> levels) {
    if (levels.isEmpty()) {
      return null;
    }

    return levels.get(levels.firstLongKey());
  }

  private Order add(
      Long2ObjectRBTreeMap<PriceLevel> levels, long orderId, Side side, long price, long size) {
    PriceLevel level = levels.get(price);
    if (level == null) {
      level = new PriceLevel(side, price);
      levels.put(price, level);
    }

    return level.add(orderId, size);
  }

  private void delete(Order order) {
    PriceLevel level = order.level();

    level.delete(order);

    if (level.isEmpty()) {
      delete(level);
    }
  }

  private void delete(PriceLevel level) {
    switch (level.side()) {
      case BUY:
        bids.remove(level.price());
        break;
      case SELL:
        asks.remove(level.price());
        break;
      default:
        // noop;
    }
  }

  @Override
  public String toString() {
    return "OrderBook [instrument=" + instrument + ", bids=" + bids + ", asks=" + asks + "]";
  }

  public Flux<AddOrder> addListener() {
    return this.addListener;
  }

  public Flux<MatchOrder> matchListener() {
    return matchListener;
  }

  public Set getAskPrices() {
    return this.asks.keySet();
  }

  public Set getBidPrices() {
    return this.bids.keySet();
  }
}
