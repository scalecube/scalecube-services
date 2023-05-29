package io.scalecube.services.gateway;

import io.scalecube.services.ServiceCall;
import java.util.StringJoiner;
import java.util.concurrent.Executor;

public class GatewayOptions implements Cloneable {

  private String id;
  private int port = 0;
  private Executor workerPool;
  private ServiceCall call;

  public GatewayOptions() {}

  public String id() {
    return id;
  }

  public GatewayOptions id(String id) {
    final GatewayOptions c = clone();
    c.id = id;
    return c;
  }

  public int port() {
    return port;
  }

  public GatewayOptions port(int port) {
    final GatewayOptions c = clone();
    c.port = port;
    return c;
  }

  public Executor workerPool() {
    return workerPool;
  }

  public GatewayOptions workerPool(Executor workerPool) {
    final GatewayOptions c = clone();
    c.workerPool = workerPool;
    return c;
  }

  public ServiceCall call() {
    return call;
  }

  public GatewayOptions call(ServiceCall call) {
    final GatewayOptions c = clone();
    c.call = call;
    return c;
  }

  @Override
  public GatewayOptions clone() {
    try {
      return (GatewayOptions) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GatewayOptions.class.getSimpleName() + "[", "]")
        .add("id='" + id + "'")
        .add("port=" + port)
        .add("workerPool=" + workerPool)
        .add("call=" + call)
        .toString();
  }
}
