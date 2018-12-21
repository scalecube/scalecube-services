package io.scalecube.services.exceptions;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import java.util.Optional;

public final class DefaultErrorMapper
    implements ServiceClientErrorMapper, ServiceProviderErrorMapper {

  public static final DefaultErrorMapper INSTANCE = new DefaultErrorMapper();

  private static final int DEFAULT_ERROR_CODE = 500;

  private DefaultErrorMapper() {
    // do not instantiate
  }

  @Override
  public Throwable toError(ServiceMessage message) {
    String qualifier = message.qualifier();
    ErrorData data = message.data();

    int errorType = Integer.parseInt(Qualifier.getQualifierAction(qualifier));
    int errorCode = data.getErrorCode();
    String errorMessage = data.getErrorMessage();

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

  @Override
  public ServiceMessage toMessage(Throwable throwable) {
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

    String errorMessage =
        Optional.ofNullable(throwable.getMessage()).orElseGet(throwable::toString);
    ErrorData errorData = new ErrorData(errorCode, errorMessage);

    return ServiceMessage.error(errorType).data(errorData).build();
  }
}
