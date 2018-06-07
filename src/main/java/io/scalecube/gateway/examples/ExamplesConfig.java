package io.scalecube.gateway.examples;

/**
 * Config properties for example services.
 */
public class ExamplesConfig {

  private int servicePort = 5801;
  private String seedAddress = "localhost:4801";

  public ExamplesConfig() {}

  public int getServicePort() {
    return servicePort;
  }

  public void setServicePort(int servicePort) {
    this.servicePort = servicePort;
  }

  public String getSeedAddress() {
    return seedAddress;
  }

  public void setSeedAddress(String seedAddress) {
    this.seedAddress = seedAddress;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ExamplesConfig{");
    sb.append("servicePort=").append(servicePort);
    sb.append(", seedAddress='").append(seedAddress).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
