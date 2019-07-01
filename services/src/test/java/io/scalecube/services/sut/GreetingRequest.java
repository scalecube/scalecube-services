package io.scalecube.services.sut;

import java.time.Duration;

public final class GreetingRequest {

  private String name;
  private Duration duration;

  public GreetingRequest() {}

  public GreetingRequest(String name) {
    this.name = name;
    this.duration = null;
  }

  public GreetingRequest(String name, Duration duration) {
    this.name = name;
    this.duration = duration;
  }

  public String getName() {
    return name;
  }

  public Duration getDuration() {
    return duration;
  }

  @Override
  public String toString() {
    return "GreetingRequest{name='" + name + '\'' + '}';
  }
}
