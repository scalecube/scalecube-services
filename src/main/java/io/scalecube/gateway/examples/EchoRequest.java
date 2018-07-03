package io.scalecube.gateway.examples;

public class EchoRequest {

  private String name;
  private long frequencyMillis;

  public EchoRequest() {}

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getFrequencyMillis() {
    return frequencyMillis;
  }

  public void setFrequencyMillis(long frequencyMillis) {
    this.frequencyMillis = frequencyMillis;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("EchoRequest{");
    sb.append("name='").append(name).append('\'');
    sb.append(", frequencyMillis=").append(frequencyMillis);
    sb.append('}');
    return sb.toString();
  }
}
