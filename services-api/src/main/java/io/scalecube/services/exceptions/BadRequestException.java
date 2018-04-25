package io.scalecube.services.exceptions;

/**
 * Means that there was something wrong with request
 */
public class BadRequestException extends ServiceException {

  public BadRequestException(int errorCode, String message) {
    super(errorCode, message);
  }
}
