package io.scalecube.services.exceptions;

public class UnauthorizedException extends ServiceException {

  public static final int ERROR_TYPE = 401;

  public UnauthorizedException(int errorCode, String message) {
    super(errorCode, message);
  }
}
