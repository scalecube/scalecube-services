package io.scalecube.services.exceptions;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;

import java.util.Optional;

public class ExceptionProcessor {

  private static final int DEFAULT_ERROR_CODE = 500;
  private static final String ERROR_NAMESPACE = "io.scalecube.service.error";

  public static boolean isError(ServiceMessage message) {
    return message.qualifier().contains(ERROR_NAMESPACE);
  }

  public static ServiceMessage toMessage(Throwable throwable) {
    int errorCode = DEFAULT_ERROR_CODE;
    int errorType = DEFAULT_ERROR_CODE;

    if (throwable instanceof ServiceException) {
      errorCode = ((ServiceException) throwable).errorCode();
      if (throwable instanceof BadRequestException) {
        errorType = BadRequestException.ERROR_TYPE;
      } else if (throwable instanceof UnauthorizedException) {
        errorType = UnauthorizedException.ERROR_TYPE;
      } else if (throwable instanceof ServiceUnavailableException) {
        errorType = ServiceUnavailableException.ERROR_TYPE;
      } else if (throwable instanceof InternalServiceException) {
        errorType = InternalServiceException.ERROR_TYPE;
      }
    }

    String errorMessage = Optional.ofNullable(throwable.getMessage()).orElseGet(throwable::toString);
    ErrorData errorData = new ErrorData(errorCode, errorMessage);

    return ServiceMessage.builder().qualifier(
        new Qualifier(ERROR_NAMESPACE, Integer.toString(errorType)).asString())
        .data(errorData)
        .build();
  }

  public static ServiceException toException(ServiceMessage message) {
    int errorType = Integer.parseInt(Qualifier.getQualifierAction(message.qualifier()));
    int errorCode = ((ErrorData) message.data()).getErrorCode();
    String errorMessage = ((ErrorData) message.data()).getErrorMessage();

    switch (errorType) {
      case BadRequestException.ERROR_TYPE:
        return new BadRequestException(errorCode, errorMessage);
      case UnauthorizedException.ERROR_TYPE:
        return new UnauthorizedException(errorCode, errorMessage);
      case ServiceUnavailableException.ERROR_TYPE:
        return new ServiceUnavailableException(errorCode, errorMessage);
      case InternalServiceException.ERROR_TYPE:
        return new InternalServiceException(errorCode, errorMessage);
      // Handle other types of Service Exceptions here
      default:
        return new InternalServiceException(errorCode, errorMessage);
    }
  }

  public static Throwable mapException(Throwable throwable) {
    String errorMessage = Optional.ofNullable(throwable.getMessage()).orElseGet(throwable::toString);
    // Handle other mappings of Service Exceptions here
    return new InternalServiceException(DEFAULT_ERROR_CODE, errorMessage);
  }
}
