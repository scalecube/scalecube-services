package io.scalecube.services.transport;

import io.scalecube.services.transport.api.ServiceTransport;
import io.scalecube.services.transport.api.WorkerThreadChooser;

public class ServiceTransportConfig {
  private Integer servicePort;
  private String serviceHost;
  private ServiceTransport transport;
  private Integer numOfThreads;
  private WorkerThreadChooser workerThreadChooser;

  private ServiceTransportConfig(Builder builder) {
    this.servicePort = builder.servicePort;
    this.serviceHost = builder.serviceHost;
    this.transport = builder.transport;
    this.numOfThreads = builder.numOfThreads;
    this.workerThreadChooser = builder.workerThreadChooser;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Integer servicePort() {
    return servicePort;
  }

  public String serviceHost() {
    return serviceHost;
  }

  public ServiceTransport transport() {
    return transport;
  }

  public Integer numOfThreads() {
    return numOfThreads;
  }

  public WorkerThreadChooser workerThreadChooser() {
    return workerThreadChooser;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("ServiceTransportConfig{");
    sb.append("servicePort=").append(servicePort);
    sb.append(", serviceHost='").append(serviceHost).append('\'');
    sb.append(", transport=").append(transport);
    sb.append(", numOfThreads=").append(numOfThreads);
    sb.append(", workerThreadChooser=").append(workerThreadChooser);
    sb.append('}');
    return sb.toString();
  }

  public static class Builder {
    private Integer servicePort;
    private String serviceHost;
    private ServiceTransport transport;
    private WorkerThreadChooser workerThreadChooser;
    private Integer numOfThreads = Runtime.getRuntime().availableProcessors();

    public Builder servicePort(Integer servicePort) {
      this.servicePort = servicePort;
      return this;
    }

    public Builder serviceHost(String serviceHost) {
      this.serviceHost = serviceHost;
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

    public Builder workerThreadChooser(WorkerThreadChooser workerThreadChooser) {
      this.workerThreadChooser = workerThreadChooser;
      return this;
    }

    public ServiceTransportConfig build() {
      return new ServiceTransportConfig(this);
    }
  }
}
