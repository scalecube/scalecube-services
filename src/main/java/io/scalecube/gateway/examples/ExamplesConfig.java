package io.scalecube.gateway.examples;

import java.util.Arrays;
import java.util.List;

/** Config properties for example services. */
public class ExamplesConfig {

  private int servicePort = 5801;
  private List<String> seeds = Arrays.asList("localhost:4801");

  public ExamplesConfig() {}

  public int getServicePort() {
    return servicePort;
  }

  public void setServicePort(int servicePort) {
    this.servicePort = servicePort;
  }

  public List<String> getSeedAddress() {
    return seeds;
  }

  public void setSeedAddress(List<String> seedAddress) {
    this.seeds = seedAddress;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ExamplesConfig{");
    sb.append("servicePort=").append(servicePort);
    sb.append(", seeds='").append(seeds).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
