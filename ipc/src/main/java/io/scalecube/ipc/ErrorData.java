package io.scalecube.ipc;

public final class ErrorData {

  public static final String ERROR_CODE_NAME = "errorCode";
  public static final String MESSAGE_NAME = "message";

  private final int errorCode;
  private final String message;

  public ErrorData(int errorCode, String message) {
    this.errorCode = errorCode;
    this.message = message;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return "ErrorData [errorCode=" + errorCode
        + ", message=" + message
        + "]";
  }
}
