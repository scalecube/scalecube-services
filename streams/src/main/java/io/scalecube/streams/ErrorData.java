package io.scalecube.streams;

import java.util.Objects;

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
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    ErrorData errorData = (ErrorData) that;
    return errorCode == errorData.errorCode && Objects.equals(message, errorData.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(errorCode, message);
  }

  @Override
  public String toString() {
    return "ErrorData [errorCode=" + errorCode
        + ", message=" + message
        + "]";
  }
}
