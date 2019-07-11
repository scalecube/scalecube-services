package io.scalecube.services.exceptions;

public class ForbiddenException extends ServiceException {

  public static final int ERROR_TYPE = 403;

  public ForbiddenException(int errorCode, String message) {
    super(errorCode, message);
  }

  public ForbiddenException(String message) {
    super(ERROR_TYPE, message);
  }

  public ForbiddenException(Throwable cause) {
    super(ERROR_TYPE, cause);
  }
}
