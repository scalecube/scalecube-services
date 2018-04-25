package io.scalecube.services.exceptions;

public class UnauthorizedException extends ServiceException {

  public UnauthorizedException(int errorCode, String message) {
    super(errorCode, message);
  }

}
