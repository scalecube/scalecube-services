package io.scalecube.services.gateway.clientsdk.exceptions.mappers;

import io.scalecube.services.api.Qualifier;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.gateway.clientsdk.ClientMessage;
import io.scalecube.services.gateway.clientsdk.ErrorData;

public class DefaultClientErrorMapper implements ClientErrorMapper {

  public static final ClientErrorMapper INSTANCE = new DefaultClientErrorMapper();

  private DefaultClientErrorMapper() {
    // do not instantiate
  }

  @Override
  public Throwable toError(ClientMessage message) {
    String qualifier = message.qualifier();
    ErrorData errorData = message.data();

    int errorType = Integer.parseInt(Qualifier.getQualifierAction(qualifier));
    int errorCode = errorData.getErrorCode();
    String errorMessage = errorData.getErrorMessage();

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
}
