package io.scalecube.services.transport.rsocket;

import io.rsocket.Payload;
import io.scalecube.services.api.ServiceMessage;

public interface PayloadCodec {

  Payload encode(ServiceMessage message);

  ServiceMessage decode(Payload payload);
}
