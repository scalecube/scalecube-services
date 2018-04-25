package io.scalecube.services.exceptions;

public class ServiceException extends RuntimeException {

  private final int errorCode;

  public ServiceException(int errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }

  public int getErrorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{errorCode=" + errorCode + ", errorMessage=" + getMessage() + '}';
  }
}
