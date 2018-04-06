package io.scalecube.streams.exceptions;

import io.scalecube.streams.Qualifier;
import io.scalecube.streams.StreamMessage;

public interface StreamExceptionMapper {

  StreamException toException(Qualifier qualifier, Object data);

  StreamMessage toMessage(Throwable throwable);
}
