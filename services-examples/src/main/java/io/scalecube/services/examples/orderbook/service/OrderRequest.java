package io.scalecube.services.examples.orderbook.service;

import io.scalecube.services.examples.orderbook.service.engine.Order;

public class OrderRequest {

  private String instrument;
  private Order order;

  public OrderRequest() {}

  public OrderRequest(Order order, String instrument) {
    this.order = order;
    this.instrument = instrument;
  }

  public Order order() {
    return order;
  }

  public String instrument() {
    return instrument;
  }
}
