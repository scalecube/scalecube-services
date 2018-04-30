package io.scalecube.services.transport.rsocket;

import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.ServiceTransport;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.rsocket.client.RSocketClientTransport;
import io.scalecube.services.transport.rsocket.server.RSocketServerTransport;
import io.scalecube.services.transport.server.api.ServerTransport;

public class RSocketServiceTransport implements ServiceTransport {

  private static final String DEFAULT_HEADERS_FORMAT = "application/json";

  @Override
  public ClientTransport getClientTransport() {
    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    return new RSocketClientTransport(new ServiceMessageCodec(headersCodec));
  }

  @Override
  public ServerTransport getServerTransport() {
    HeadersCodec headersCodec = HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT);
    return new RSocketServerTransport(new ServiceMessageCodec(headersCodec));
  }

}
