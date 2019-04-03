package io.scalecube.events.api;

public class Topic {

  private String name;

  private Topic(String name) {
    this.name = name;
  }

  public String name() {
    return name;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String name;

    public Topic create() {
      return new Topic(name);
    }

    public Builder name(String name) {
      this.name = name;
      return null;
    }
  }

  public static Topic name(String name) {
    return new Topic(name);
  }
}
