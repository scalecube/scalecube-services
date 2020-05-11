package io.scalecube.services.gateway;

import io.scalecube.services.ServiceCall;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class GatewayOptions {

  private Executor workerPool;
  private ServiceCall call;
  private String id;
  private int port = 0;

  public GatewayOptions() {}

  /**
   * GatewayOptions copy contractor.
   *
   * @param other GatewayOptions to copy.
   */
  public GatewayOptions(GatewayOptions other) {
    this.id = other.id;
    this.port = other.port;
    this.workerPool = other.workerPool;
    this.call = other.call;
  }

  private GatewayOptions set(Consumer<GatewayOptions> c) {
    GatewayOptions s = new GatewayOptions(this);
    c.accept(s);
    return s;
  }

  public GatewayOptions id(String id) {
    return set(o -> o.id = id);
  }

  public String id() {
    return id;
  }

  public GatewayOptions port(int port) {
    return set(o -> o.port = port);
  }

  public int port() {
    return port;
  }

  public GatewayOptions workerPool(Executor workerPool) {
    return set(o -> o.workerPool = workerPool);
  }

  public Executor workerPool() {
    return workerPool;
  }

  public GatewayOptions call(ServiceCall call) {
    return set(o -> o.call = call);
  }

  public ServiceCall call() {
    return call;
  }
}
