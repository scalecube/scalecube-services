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

  private final ServiceMessageCodec messageCodec;
  private final LoopResources loopResources;

  public RSocketServiceTransport() {
    HeadersCodec headersCodec = HeadersCodec.getInstance("application/json");
    messageCodec =  new ServiceMessageCodec(headersCodec);
    loopResources =  LoopResources.create("rsocket-worker");
  }

  public RSocketServiceTransport(HeadersCodec headersCodec, LoopResources loopResources) {
    this.messageCodec = new ServiceMessageCodec(headersCodec);
    this.loopResources = loopResources;
  }

  @Override
  public ClientTransport clientTransport(TransportResources resources) {
    return new RSocketClientTransport(
            messageCodec,
        ((RSocketTransportResources) resources)
            .workerPool()
            .<LoopResources>map(DelegatedLoopResources::newClientLoopResources)
            .orElse(loopResources));
  }

  @Override
  public ServerTransport serverTransport(TransportResources resources) {
    return new RSocketServerTransport(
            messageCodec,
        ((RSocketTransportResources) resources)
            .workerPool()
            .<LoopResources>map(DelegatedLoopResources::newServerLoopResources)
            .orElse(loopResources));
  }
}
