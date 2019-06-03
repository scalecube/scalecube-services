package io.scalecube.services.transport.rsocket;

import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransport;
import io.scalecube.services.transport.api.TransportResources;
import reactor.netty.resources.LoopResources;

/**
 * RSocket service transport. Entry point for getting {@link RSocketClientTransport} and {@link
 * RSocketServerTransport}.
 */
public class RSocketServiceTransport implements ServiceTransport {

  public static final RSocketServiceTransport INSTANCE = new RSocketServiceTransport();

  private static final HeadersCodec HEADERS_CODEC = HeadersCodec.getInstance("application/json");
  private static final ServiceMessageCodec MESSAGE_CODEC = new ServiceMessageCodec(HEADERS_CODEC);
  private static final LoopResources LOOP_RESOURCES = LoopResources.create("rsocket-worker");

  @Override
  public ClientTransport clientTransport(TransportResources resources) {
    return new RSocketClientTransport(
        MESSAGE_CODEC,
        ((RSocketTransportResources) resources)
            .workerPool()
            .<LoopResources>map(DelegatedLoopResources::newClientLoopResources)
            .orElse(LOOP_RESOURCES));
  }

  @Override
  public ServerTransport serverTransport(TransportResources resources) {
    return new RSocketServerTransport(
        MESSAGE_CODEC,
        ((RSocketTransportResources) resources)
            .workerPool()
            .<LoopResources>map(DelegatedLoopResources::newServerLoopResources)
            .orElse(LOOP_RESOURCES));
  }
}
