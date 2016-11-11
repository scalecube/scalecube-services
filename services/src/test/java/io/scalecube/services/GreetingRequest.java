package io.scalecube.services;

import java.time.Duration;

final class GreetingRequest {

  private final String name;
  private final Duration duration;

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
