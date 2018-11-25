package io.scalecube.services.gateway;

import io.micrometer.core.instrument.Counter;
import io.scalecube.services.metrics.Metrics;
import java.util.concurrent.atomic.LongAdder;

public class GatewayMetrics {

  public static final String METRIC_CONNECTION = "connection";
  public static final String METRIC_REQ = "request";
  public static final String METRIC_RESP = "response";
  public static final String METRIC_SERVICE_RESP = "service-response";

  private final LongAdder connectionCounter;
  private final Counter requestCounter;
  private final Counter responseCounter;
  private final Counter serviceResponseCounter;

  /**
   * Constructor for gateway metrics.
   *
   * @param prefix prefix for gateway metrics instance
   * @param metrics microservices metrics
   */
  public GatewayMetrics(String prefix, Metrics metrics) {
    connectionCounter =
        metrics != null ? metrics.gauge(new LongAdder(), prefix, METRIC_CONNECTION) : null;
    requestCounter = metrics != null ? metrics.getCounter(prefix, "", METRIC_REQ) : null;
    responseCounter = metrics != null ? metrics.getCounter(prefix, "", METRIC_RESP) : null;
    serviceResponseCounter =
        metrics != null ? metrics.getCounter(prefix, "", METRIC_SERVICE_RESP) : null;
  }

  /** Increment connection counter. */
  public void incConnection() {
    if (connectionCounter != null) {
      connectionCounter.increment();
    }
  }

  /** Decrement connection counter. */
  public void decConnection() {
    if (connectionCounter != null) {
      connectionCounter.decrement();
    }
  }

  /** Mark request for calls/sec measurement. */
  public void markRequest() {
    if (requestCounter != null) {
      requestCounter.increment();
    }
  }

  /** Mark response for calls/sec measurement. */
  public void markResponse() {
    if (responseCounter != null) {
      responseCounter.increment();
    }
  }

  /** Mark service response for calls/sec measurement. */
  public void markServiceResponse() {
    if (serviceResponseCounter != null) {
      serviceResponseCounter.increment();
    }
  }
}
