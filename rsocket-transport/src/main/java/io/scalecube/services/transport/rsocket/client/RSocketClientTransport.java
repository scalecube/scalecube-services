package io.scalecube.services.transport.rsocket.client;

import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.scalecube.services.transport.client.api.ClientChannel;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.rsocket.PayloadCodec;
import io.scalecube.transport.Address;

public class RSocketClientTransport implements ClientTransport {

  private PayloadCodec payloadCodec;

  public RSocketClientTransport(PayloadCodec payloadCodec) {
    this.payloadCodec = payloadCodec;
  }

  @Override
  public ClientChannel create(Address address) {
    return new RSocketServiceClientAdapter(RSocketFactory.connect()
        .transport(TcpClientTransport.create(address.host(), address.port()))
        .start()
        .block(), payloadCodec);

  }

}
