package io.scalecube.services.transport.rsocket;

import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.transport.ServiceTransport;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.rsocket.client.RSocketClientTransport;
import io.scalecube.services.transport.rsocket.server.RSocketServerTransport;
import io.scalecube.services.transport.server.api.ServerTransport;

import java.util.HashMap;
import java.util.Map;

public class RSocketServiceTransport implements ServiceTransport {

  private final ServiceMessageCodec codec;

  public RSocketServiceTransport() {
    this.codec = null;
  }

  @Override
  public ClientTransport getClientTransport() {
    return new RSocketClientTransport(this.codec);
  }

  @Override
  public ServerTransport getServerTransport() {
    return new RSocketServerTransport(this.codec);
  }

  @Override
  public Map<String, ? extends ServiceMessageCodec> getMessageCodecs() {
    Map<String, ServiceMessageCodec> codecs = new HashMap<>();
    codecs.put("application/json", codec);
    return codecs;
  }

}
