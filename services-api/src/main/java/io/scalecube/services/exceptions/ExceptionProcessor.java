package io.scalecube.services.exceptions;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;

public class ExceptionProcessor {

  // Error types
  private static final int GENERAL_SERVICE_ERROR = 503;
  private static final int UNAUTHORIZED_SERVICE_ERROR = 401;
  private static final int BAD_REQUEST_ERROR = 400;

  private static final String ERROR_NAMESPACE = "io.scalecube.service.error";

  public static boolean isError(ServiceMessage message) {
    return message.qualifier().contains(ERROR_NAMESPACE);
  }

  public static ServiceMessage toMessage(Throwable throwable) {
    String errorMessage = throwable.getMessage();
    int errorCode = GENERAL_SERVICE_ERROR;
    int errorType = GENERAL_SERVICE_ERROR;
    if (throwable instanceof ServiceException) {
      errorCode = ((ServiceException) throwable).getErrorCode();
      if (throwable instanceof BadRequestException) {
        errorType = BAD_REQUEST_ERROR;
      } else if (throwable instanceof UnauthorizedException) {
        errorType = UNAUTHORIZED_SERVICE_ERROR;
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
      case BAD_REQUEST_ERROR:
        return new BadRequestException(errorCode, errorMessage);
      case UNAUTHORIZED_SERVICE_ERROR:
        return new UnauthorizedException(errorCode, errorMessage);
      // Handle other types of Service Exceptions here
      default:
        return new ServiceException(errorCode, errorMessage);
    }
  }
}
