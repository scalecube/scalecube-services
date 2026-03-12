package io.scalecube.services.exceptions;

import java.util.StringJoiner;

public abstract class ServiceException extends RuntimeException {

  private final int errorCode;

  public ServiceException(int errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public ServiceException(int errorCode, Throwable cause) {
    super(cause.getMessage(), cause);
    this.errorCode = errorCode;
  }

  public ServiceException(int errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }

  public int errorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", getClass().getSimpleName() + "[", "]")
        .add("errorCode=" + errorCode)
        .add("errorMessage='" + getMessage() + "'")
        .toString();
  }
}
