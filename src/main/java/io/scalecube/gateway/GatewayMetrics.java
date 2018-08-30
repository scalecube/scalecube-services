package io.scalecube.gateway;

import io.scalecube.services.metrics.Metrics;

public class GatewayMetrics {

  public static final String METRIC_CONNECTIONS = "connections";
  public static final String METRIC_REQUESTS = "requests";
  public static final String METRIC_RESPONSES = "responses";

  private final String prefix;
  private final Metrics metrics;

  /**
   * Constructor for gatewat metrics.
   *
   * @param prefix prefix for gateway metrics instance
   * @param metrics microservices metrics
   */
  public GatewayMetrics(String prefix, Metrics metrics) {
    this.prefix = prefix;
    this.metrics = metrics;
  }

  /** Increment connection counter. */
  public void incrConnection() {
    if (metrics != null) {
      metrics.getCounter(prefix, METRIC_CONNECTIONS).inc();
    }
  }

  /** Decrement connection counter. */
  public void decrConnection() {
    if (metrics != null) {
      metrics.getCounter(prefix, METRIC_CONNECTIONS).dec();
    }
  }

  /** Mark request for calls/sec measurement. */
  public void markRequest() {
    if (metrics != null) {
      metrics.getMeter(prefix, "", METRIC_REQUESTS).mark();
    }
  }

  /** Mark response for calls/sec measurement. */
  public void markResponse() {
    if (metrics != null) {
      metrics.getMeter(prefix, "", METRIC_RESPONSES).mark();
    }
  }
}
