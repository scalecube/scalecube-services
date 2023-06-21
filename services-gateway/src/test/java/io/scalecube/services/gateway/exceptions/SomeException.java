package io.scalecube.services.gateway.exceptions;

import io.scalecube.services.exceptions.ServiceException;

public class SomeException extends ServiceException {

  public static final int ERROR_TYPE = 4020;
  public static final int ERROR_CODE = 42;
  public static final String ERROR_MESSAGE = "smth happened";

  public SomeException() {
    super(ERROR_CODE, ERROR_MESSAGE);
  }
}
