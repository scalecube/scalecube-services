package io.scalecube.gateway.clientsdk.exceptions;

import io.scalecube.services.api.Qualifier;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.InternalServiceException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.exceptions.UnauthorizedException;

public final class ExceptionProcessor {

  /** Default constructor. */
  private ExceptionProcessor() {
    // Do not instantiate
  }

  /**
   * Boolean function telling is given qualifier string an error qualifier. See {@link
   * Qualifier#ERROR_NAMESPACE}.
   *
   * @param qualifier qualifier string.
   * @return true if qualifier given is error qualifier
   */
  public static boolean isError(String qualifier) {
    return qualifier.contains(Qualifier.ERROR_NAMESPACE);
  }

  /**
   * Exception converter to {@link ServiceException}.
   *
   * @param qualifier qualifier string.
   * @param errorCode error code.
   * @param errorMessage error message.
   * @return service exception instance.
   */
  public static ServiceException toException(String qualifier, int errorCode, String errorMessage) {
    int errorType = Integer.parseInt(Qualifier.getQualifierAction(qualifier));

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
