package io.scalecube.services.sut;

import java.time.Duration;
import java.util.StringJoiner;

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

  public String name() {
    return name;
  }

  public GreetingRequest name(String name) {
    this.name = name;
    return this;
  }

  public Duration duration() {
    return duration;
  }

  public GreetingRequest duration(Duration duration) {
    this.duration = duration;
    return this;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", GreetingRequest.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .add("duration=" + duration)
        .toString();
  }
}
