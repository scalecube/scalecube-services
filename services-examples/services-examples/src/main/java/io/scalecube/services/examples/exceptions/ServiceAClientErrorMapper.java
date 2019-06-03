package io.scalecube.services.examples.exceptions;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceClientErrorMapper;

public class ServiceAClientErrorMapper implements ServiceClientErrorMapper {

  @Override
  public Throwable toError(ServiceMessage message) {
    ErrorData data = message.data();

    if (data.getErrorCode() == 42) {
      // implement service mapping logic
      return new ServiceAException(data.getErrorMessage());
    } else {
      // or delegate it to default mapper
      return DefaultErrorMapper.INSTANCE.toError(message);
    }
  }
}
