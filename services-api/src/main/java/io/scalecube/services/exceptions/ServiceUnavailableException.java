package io.scalecube.services.exceptions;

public class ServiceUnavailableException extends ServiceException {

  public final static int ERROR_TYPE = 503;

  public ServiceUnavailableException(int errorCode, String message) {
    super(errorCode, message);
  }
}
