package io.scalecube.services.transport.client.api;

import io.scalecube.services.codecs.api.MessageCodec;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.transport.Address;

public interface ClientTransport {

  ClientChannel create(Address address);

  MessageCodec getMessageCodec();

  ServiceMessageDataCodec getServiceMessageDataCodec();

}
