package io.scalecube.services.transport.rsocket;

import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.transport.ServiceTransport;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.rsocket.client.RSocketClientTransport;
import io.scalecube.services.transport.rsocket.server.RSocketServerTransport;
import io.scalecube.services.transport.server.api.ServerTransport;

public class RSocketServicesTransport implements ServiceTransport{

 
  private ServiceMessageCodec codec = new RSocketJsonPayloadCodec();

  @Override
  public ClientTransport getClientTransport() {
    return new RSocketClientTransport(this.codec);
  }

  @Override
  public ServerTransport getServerTransport() {
    return new RSocketServerTransport(this.codec);
  }

}
