package io.scalecube.services.examples.exceptions;

public class ServiceAException extends Exception {

  private static final int ERROR_CODE = 42;

  ServiceAException(String message) {
    super(message);
  }

  public int code() {
    return ERROR_CODE;
  }
}
