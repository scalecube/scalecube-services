package io.scalecube.services.gateway.clientsdk.websocket;

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

  public String codeAsString() {
    return String.valueOf(code);
  }

  /**
   * Return appropriate instance of {@link Signal} for given signal code.
   *
   * @param code signal code
   * @return signal instance
   */
  public static Signal from(String code) {
    return from(Integer.parseInt(code));
  }

  /**
   * Return appropriate instance of {@link Signal} for given signal code.
   *
   * @param code signal code
   * @return signal instance
   */
  public static Signal from(int code) {
    switch (code) {
      case 1:
        return COMPLETE;
      case 2:
        return ERROR;
      case 3:
        return CANCEL;
      default:
        throw new IllegalArgumentException("Unknown signal: " + code);
    }
  }
}
