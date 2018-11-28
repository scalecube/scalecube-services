package io.scalecube.services.transport;

import io.scalecube.services.transport.api.ServiceTransport;
import java.util.function.Consumer;

public class ServiceTransportConfig {
  private final Integer port;
  private final String host;
  private final ServiceTransport transport;
  private final Integer numOfThreads;

  private ServiceTransportConfig(Builder builder) {
    this.port = builder.port;
    this.host = builder.host;
    this.transport = builder.transport;
    this.numOfThreads = builder.numOfThreads;
  }

  /**
   * Returns a new transport config builder and apply to it the given transport options.
   *
   * @param transportOptions transport options
   * @return transport config builder
   */
  public static Builder builder(Consumer<Builder> transportOptions) {
    Builder builder = new Builder();
    if (transportOptions != null) {
      transportOptions.accept(builder);
    }
    return builder;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Integer port() {
    return port;
  }

  public String host() {
    return host;
  }

  public ServiceTransport transport() {
    return transport;
  }

  public Integer numOfThreads() {
    return numOfThreads;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("ServiceTransportConfig{");
    sb.append("port=").append(port);
    sb.append(", host='").append(host).append('\'');
    sb.append(", transport=").append(transport);
    sb.append(", numOfThreads=").append(numOfThreads);
    sb.append('}');
    return sb.toString();
  }

  public static class Builder {
    private Integer port;
    private String host;
    private ServiceTransport transport;
    private Integer numOfThreads = Runtime.getRuntime().availableProcessors();

    public Builder port(Integer port) {
      this.port = port;
      return this;
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder transport(ServiceTransport transport) {
      this.transport = transport;
      return this;
    }

    public Builder numOfThreads(Integer numOfThreads) {
      this.numOfThreads = numOfThreads;
      return this;
    }

    public ServiceTransportConfig build() {
      return new ServiceTransportConfig(this);
    }
  }
}
