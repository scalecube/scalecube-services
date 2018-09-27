package io.scalecube.gateway;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import io.scalecube.services.metrics.Metrics;

public class GatewayMetrics {

  public static final String METRIC_CONNECTION = "connection";
  public static final String METRIC_REQ = "request";
  public static final String METRIC_RESP = "response";
  public static final String METRIC_SERVICE_RESP = "service-response";

  private final Counter connCounter;
  private final Meter reqMeter;
  private final Meter respMeter;
  private final Meter serviceRespMeter;

  /**
   * Constructor for gateway metrics.
   *
   * @param prefix prefix for gateway metrics instance
   * @param metrics microservices metrics
   */
  public GatewayMetrics(String prefix, Metrics metrics) {
    connCounter = metrics != null ? metrics.getCounter(prefix, METRIC_CONNECTION) : null;
    reqMeter = metrics != null ? metrics.getMeter(prefix, "", METRIC_REQ) : null;
    respMeter = metrics != null ? metrics.getMeter(prefix, "", METRIC_RESP) : null;
    serviceRespMeter = metrics != null ? metrics.getMeter(prefix, "", METRIC_SERVICE_RESP) : null;
  }

  /** Increment connection counter. */
  public void incConnection() {
    if (connCounter != null) {
      connCounter.inc();
    }
  }

  /** Decrement connection counter. */
  public void decConnection() {
    if (connCounter != null) {
      connCounter.dec();
    }
  }

  /** Mark request for calls/sec measurement. */
  public void markRequest() {
    if (reqMeter != null) {
      reqMeter.mark();
    }
  }

  /** Mark response for calls/sec measurement. */
  public void markResponse() {
    if (respMeter != null) {
      respMeter.mark();
    }
  }

  /** Mark service response for calls/sec measurement. */
  public void markServiceResponse() {
    if (serviceRespMeter != null) {
      serviceRespMeter.mark();
    }
  }
}
