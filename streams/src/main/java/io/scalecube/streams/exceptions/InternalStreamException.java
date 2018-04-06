package io.scalecube.streams.exceptions;

import io.scalecube.streams.ErrorData;

public class InternalStreamException extends StreamException {

  public InternalStreamException(int code, String message) {
    super(code, message);
  }

  public InternalStreamException(ErrorData errorData) {
    super(errorData.getErrorCode(), errorData.getMessage());
  }
}
