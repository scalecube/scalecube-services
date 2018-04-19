package io.scalecube.rsockets;

import io.rsocket.Payload;
import io.scalecube.services.api.ServiceMessage;

public interface PayloadCodec {

  Payload encode(ServiceMessage message);

  ServiceMessage decode(Payload payload);

  ServiceMessage decode(Payload payload, Class<?> dataType);



}
