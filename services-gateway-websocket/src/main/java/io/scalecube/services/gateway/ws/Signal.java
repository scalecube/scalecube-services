package io.scalecube.services.gateway.ws;

public enum Signal {
  COMPLETE(1),
  ERROR(2),
  CANCEL(3);

  private final int code;

  Signal(int code) {
    this.code = code;
  }

  public int code() {
    return code;
  }
}
