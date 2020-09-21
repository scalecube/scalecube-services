package io.scalecube.services.examples.orderbook.service.engine;

import io.scalecube.services.examples.orderbook.service.engine.events.MatchOrder;
import io.scalecube.services.examples.orderbook.service.engine.events.Side;
import java.util.ArrayList;
import org.reactivestreams.Processor;
import reactor.core.publisher.EmitterProcessor;

public class PriceLevel {

  private Side side;

  private long price;

  private ArrayList<Order> orders;

  /**
   * Create a new price level.
   *
   * @param side the side of this level (either {@link Side#BUY} or {@link Side#SELL})
   * @param price the price this level should hold.
   */
  public PriceLevel(Side side, long price) {
    this.side = side;
    this.price = price;
    this.orders = new ArrayList<>();
  }

  public Side side() {
    return side;
  }

  public long price() {
    return price;
  }

  public boolean isEmpty() {
    return orders.isEmpty();
  }

  /**
   * Add a new order.
   *
   * @param orderId the order id
   * @param size the size
   * @return the order added to this price level
   */
  public Order add(long orderId, long size) {
    Order order = new Order(this, orderId, size);
    orders.add(order);
    return order;
  }

  /**
   * Match order if possible.
   *
   * @param orderId the incoming order id
   * @param side the incoming order side
   * @param size incoming order quantity
   * @param matchEmmiter an emitter to be notified for matches {@link Processor#onNext(Object)}
   * @return the remaining quantity of the incoming order
   */
  public long match(long orderId, Side side, long size, EmitterProcessor<MatchOrder> matchEmmiter) {
    long quantity = size;
    while (quantity > 0 && !orders.isEmpty()) {
      Order order = orders.get(0);
      long orderQuantity = order.size();
      if (orderQuantity > quantity) {
        order.reduce(quantity);
        matchEmmiter.onNext(
            new MatchOrder(order.id(), orderId, side, price, quantity, order.size()));
        quantity = 0;
      } else {
        orders.remove(0);
        matchEmmiter.onNext(new MatchOrder(order.id(), orderId, side, price, orderQuantity, 0));
        quantity -= orderQuantity;
      }
    }
    return quantity;
  }

  public void delete(Order order) {
    orders.remove(order);
  }
}
