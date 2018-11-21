package io.scalecube.services;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;

public interface ServiceErrors {

  ServiceMessage EMPTY_RESPONSE_ERROR =
      ServiceMessage.builder()
          .qualifier(Qualifier.asError(503))
          .data(new ErrorData(503, "Unexpected empty response"))
          .build();
}
