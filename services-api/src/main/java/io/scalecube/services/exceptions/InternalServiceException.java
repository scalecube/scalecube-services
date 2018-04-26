package io.scalecube.services.exceptions;

public class InternalServiceException extends ServiceException {

  public static final int ERROR_TYPE = 500;

  public InternalServiceException(int errorCode, String message) {
    super(errorCode, message);
  }
}
