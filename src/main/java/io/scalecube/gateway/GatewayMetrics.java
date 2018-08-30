package io.scalecube.gateway;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import io.scalecube.services.metrics.Metrics;

public class GatewayMetrics {

  public static final String METRIC_CONNECTIONS = "connections";
  public static final String METRIC_REQUESTS = "requests";
  public static final String METRIC_RESPONSES = "responses";

  private final String prefix;
  private final Counter connectionCounter;
  Metrics metrics;
  private Meter requestMeter;
  private Meter responseMeter;

  /**
   * Constructor for gatewat metrics.
   *
   * @param prefix prefix for gateway metrics instance
   * @param metrics microservices metrics
   */
  public GatewayMetrics(String prefix, Metrics metrics) {
    this.prefix = prefix;
    connectionCounter = metrics != null ? metrics.getCounter(prefix, METRIC_CONNECTIONS) : null;
    requestMeter = metrics != null ? metrics.getMeter(prefix, "", METRIC_REQUESTS) : null;
    responseMeter = metrics != null ? metrics.getMeter(prefix, "", METRIC_RESPONSES) : null;
  }

  /** Increment connection counter. */
  public void incrConnection() {
    if (connectionCounter != null) {
      connectionCounter.inc();
    }
  }

  /** Decrement connection counter. */
  public void decrConnection() {
    if (connectionCounter != null) {
      connectionCounter.dec();
    }
  }

  /** Mark request for calls/sec measurement. */
  public void markRequest() {
    if (requestMeter != null) {
      requestMeter.mark();
    }
  }

  /** Mark response for calls/sec measurement. */
  public void markResponse() {
    if (responseMeter != null) {
      responseMeter.mark();
    }
  }
}
