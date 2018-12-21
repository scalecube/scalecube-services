package io.scalecube.services.examples.exceptions;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceClientErrorMapper;
import reactor.core.Exceptions;

public class ServiceAClientErrorMapper implements ServiceClientErrorMapper {

  @Override
  public Throwable toError(ServiceMessage message) {
    ErrorData data = message.data();

    if (data.getErrorCode() == 42) {
      // implement service mapping logic
      throw Exceptions.propagate(new ServiceAException(data.getErrorMessage()));
    } else {
      // or delegate it to default mapper
      throw Exceptions.propagate(DefaultErrorMapper.INSTANCE.toError(message));
    }
  }
}
