package io.scalecube.services;

public class Tag {
  public Tag() {
    // default contractor for serialization
  }

  private String key;
  private String value;

  public Tag(String key, String value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public String toString() {
    return "Tag [key=" + key + ", value=" + value + "]";
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  private void setValue(String value) {
    this.value = value;
  }

  private void setKey(String key) {
    this.key = key;
  }
}
