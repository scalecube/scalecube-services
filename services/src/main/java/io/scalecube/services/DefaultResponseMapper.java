package io.scalecube.services;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;

public final class DefaultResponseMapper implements ResponseMapper {

  public static final DefaultResponseMapper DEFAULT_INSTANCE = new DefaultResponseMapper();

  private final ServiceMessageDataCodec dataCodec = new ServiceMessageDataCodec();

  private DefaultResponseMapper() {
    // Do not instantiate
  }

  @Override
  public ServiceMessage apply(ServiceMessage message, Class<?> responseType) {
    if (ExceptionProcessor.isError(message)) {
      throw ExceptionProcessor.toException(dataCodec.decode(message, ErrorData.class));
    } else {
      return dataCodec.decode(message, responseType);
    }
  }
}
