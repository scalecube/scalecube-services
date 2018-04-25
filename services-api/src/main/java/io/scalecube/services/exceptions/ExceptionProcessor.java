package io.scalecube.services.exceptions;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;

import java.util.Optional;

public class ExceptionProcessor {

  private static final String ERROR_NAMESPACE = "io.scalecube.service.error";

  public static boolean isError(ServiceMessage message) {
    return message.qualifier().contains(ERROR_NAMESPACE);
  }

  public static ServiceMessage toMessage(Throwable throwable) {
    String errorMessage = Optional.ofNullable(throwable.getMessage()).orElse(throwable.toString());
    int errorCode = ServiceException.ERROR_TYPE;
    int errorType = ServiceException.ERROR_TYPE;
    if (throwable instanceof ServiceException) {
      errorCode = ((ServiceException) throwable).getErrorCode();
      if (throwable instanceof BadRequestException) {
        errorType = BadRequestException.ERROR_TYPE;
      } else if (throwable instanceof UnauthorizedException) {
        errorType = UnauthorizedException.ERROR_TYPE;
      }
    }
    ErrorData errorData = new ErrorData(errorCode, errorMessage);
    return ServiceMessage.builder().qualifier(
        new Qualifier(ERROR_NAMESPACE, Integer.toString(errorType)).asString())
        .data(errorData)
        .build();
  }

  public static ServiceException toException(ServiceMessage message) {
    int statusCode = Integer.parseInt(Qualifier.getQualifierAction(message.qualifier()));
    int errorCode = ((ErrorData) message.data()).getErrorCode();
    String errorMessage = ((ErrorData) message.data()).getErrorMessage();
    switch (statusCode) {
      case BadRequestException.ERROR_TYPE:
        return new BadRequestException(errorCode, errorMessage);
      case UnauthorizedException.ERROR_TYPE:
        return new UnauthorizedException(errorCode, errorMessage);
      // Handle other types of Service Exceptions here
      default:
        return new ServiceException(errorCode, errorMessage);
    }
  }
}
