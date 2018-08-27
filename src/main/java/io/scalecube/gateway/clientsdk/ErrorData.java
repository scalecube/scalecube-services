package io.scalecube.gateway.clientsdk;

public final class ErrorData {

  private int errorCode;
  private String errorMessage;

  /**
   * @deprecated exposed only for de/serialization purpose.
   */
  public ErrorData() {}

  public ErrorData(int errorCode, String errorMessage) {
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public String toString() {
    return "ErrorData{" + "errorCode=" + errorCode + ", errorMessage='" + errorMessage + '\'' + '}';
  }
}
