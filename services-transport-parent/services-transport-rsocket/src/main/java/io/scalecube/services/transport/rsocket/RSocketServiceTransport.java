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

  /**
   * Default Instance.
   */
  public static final RSocketServiceTransport INSTANCE = new RSocketServiceTransport();

  private final ServiceMessageCodec messageCodec;
  private final LoopResources loopResources;

  /**
   * Default instance.
   */
  public RSocketServiceTransport() {
    messageCodec = new ServiceMessageCodec();
    loopResources = LoopResources.create("rsocket-worker");
  }

  /**
   * Constructor with DI.
   *
   * @param headersCodec  user's headers codec
   * @param loopResources loopResources
   */
  public RSocketServiceTransport(HeadersCodec headersCodec, LoopResources loopResources) {
    this.messageCodec = new ServiceMessageCodec(headersCodec);
    this.loopResources = loopResources;
  }

  /**
   * Fabric method for client transport.
   *
   * @param resources service transport resources
   * @return client's transport
   */
  @Override
  public ClientTransport clientTransport(TransportResources resources) {
    return new RSocketClientTransport(
        messageCodec,
        ((RSocketTransportResources) resources)
            .workerPool()
            .<LoopResources>map(DelegatedLoopResources::newClientLoopResources)
            .orElse(loopResources));
  }

  /**
   * Fabric method for server transport.
   *
   * @param resources service transport resources
   * @return server's transport
   */
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
