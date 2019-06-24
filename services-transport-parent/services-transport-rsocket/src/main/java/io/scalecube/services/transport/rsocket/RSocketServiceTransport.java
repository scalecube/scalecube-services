package io.scalecube.services.transport.rsocket;

import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransport;
import reactor.netty.resources.LoopResources;

/** RSocket service transport. */
public class RSocketServiceTransport implements ServiceTransport<RSocketTransportResources> {

  private static final RSocketTransportResources RESOURCES = new RSocketTransportResources();
  private static final HeadersCodec HEADERS_CODEC = HeadersCodec.getInstance("application/json");

  private final ServiceMessageCodec messageCodec;
  private final RSocketTransportResources resources;

  /**
   * Constructor with default {@code RSocketTransportResources} and default {@code HeadersCodec}.
   */
  public RSocketServiceTransport() {
    this(RESOURCES, HEADERS_CODEC);
  }

  /**
   * Constructor with DI.
   *
   * @param resources transport resources factory
   * @param headersCodec headers codec
   */
  public RSocketServiceTransport(RSocketTransportResources resources, HeadersCodec headersCodec) {
    this.resources = resources;
    this.messageCodec = new ServiceMessageCodec(headersCodec);
  }

  /**
   * Provider for rsocket transport resources.
   *
   * @return rsocket transport resources
   */
  @Override
  public RSocketTransportResources transportResources() {
    return resources;
  }

  /**
   * Fabric method for client transport.
   *
   * @return client transport
   */
  @Override
  public ClientTransport clientTransport() {
    return new RSocketClientTransport(
        messageCodec,
        resources
            .workerPool()
            .<LoopResources>map(DelegatedLoopResources::newClientLoopResources)
            .get());
  }

  /**
   * Fabric method for server transport.
   *
   * @return server transport
   */
  @Override
  public ServerTransport serverTransport() {
    return new RSocketServerTransport(
        messageCodec,
        resources
            .workerPool()
            .<LoopResources>map(DelegatedLoopResources::newServerLoopResources)
            .get());
  }
}
