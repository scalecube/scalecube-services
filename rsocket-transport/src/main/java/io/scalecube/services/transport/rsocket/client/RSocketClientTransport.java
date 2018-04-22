package io.scalecube.services.transport.rsocket.client;

import io.scalecube.services.codecs.api.MessageCodec;
import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.transport.client.api.ClientChannel;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.transport.Address;

import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;

public class RSocketClientTransport implements ClientTransport {

  private MessageCodec payloadCodec;

  public RSocketClientTransport(MessageCodec payloadCodec) {
    this.payloadCodec = payloadCodec;
  }

  @Override
  public ClientChannel create(Address address) {
    return new RSocketServiceClientAdapter(RSocketFactory.connect()
        .transport(TcpClientTransport.create(address.host(), address.port()))
        .start()
        .block(), (ServiceMessageCodec) payloadCodec);

  }

  @Override
  public MessageCodec getMessageCodec() {
    return payloadCodec;
  }

  @Override
  public ServiceMessageDataCodec getServiceMessageDataCodec() {
    return (ServiceMessageDataCodec) payloadCodec;
  }

}
