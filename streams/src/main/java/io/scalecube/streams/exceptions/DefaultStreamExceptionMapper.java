package io.scalecube.streams.exceptions;

import io.scalecube.streams.ErrorData;
import io.scalecube.streams.Qualifier;
import io.scalecube.streams.StreamMessage;

public class DefaultStreamExceptionMapper implements StreamExceptionMapper {

  private static final Qualifier DEFAULT_ERROR_QUALIFIER = Qualifier.error(500);
  private static final ErrorData UNKNOWN_ERROR_DATA = new ErrorData(0, "unknown exception");
  private static final StreamException UNKNOWN_EXCEPTION = new InternalStreamException(UNKNOWN_ERROR_DATA);

  @Override
  public StreamException toException(Qualifier qualifier, Object data) {
    if (ErrorData.class.isInstance(data)) {
      ErrorData errorData = (ErrorData) data;
      return new InternalStreamException(errorData.getErrorCode(), errorData.getMessage());
    }
    return UNKNOWN_EXCEPTION;
  }

  @Override
  public StreamMessage toMessage(Throwable throwable) {
    if (StreamException.class.isInstance(throwable)) {
      StreamException exception = (StreamException) throwable;
      return StreamMessage.builder()
          .qualifier(DEFAULT_ERROR_QUALIFIER)
          .data(new ErrorData(exception.code, exception.message))
          .build();
    }
    return StreamMessage.builder()
        .qualifier(DEFAULT_ERROR_QUALIFIER)
        .data(new ErrorData(0, throwable.getMessage()))
        .build();
  }
}
