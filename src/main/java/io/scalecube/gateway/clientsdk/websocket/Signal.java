package io.scalecube.gateway.clientsdk.websocket;

import java.util.Arrays;

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

  public static Signal from(String code) {
    return from(Integer.parseInt(code));
  }

  public static Signal from(int code) {
    return Arrays.stream(values())
        .filter(s -> s.code == code)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown signal: " + code));
  }
}
