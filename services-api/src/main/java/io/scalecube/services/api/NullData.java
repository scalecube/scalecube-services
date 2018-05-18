package io.scalecube.services.api;

public final class NullData {

  public static final NullData NULL_DATA = new NullData();

  private NullData() {
    // Do not instantiate
  }

  @Override
  public String toString() {
    return "NullData{}";
  }
}
