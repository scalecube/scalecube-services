package io.scalecube.services.transport.rsocket;

import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransportFactory;
import reactor.netty.resources.LoopResources;

/**
 * RSocket service transport. Entry point for getting {@link RSocketClientTransport} and {@link
 * RSocketServerTransport}.
 */
public class RSocketServiceTransportFactory implements
    ServiceTransportFactory<RSocketTransportResources> {

  /**
   * Default Instance.
   */
  public static final RSocketServiceTransportFactory INSTANCE =
      new RSocketServiceTransportFactory();

  private final ServiceMessageCodec messageCodec;

  /**
   * Default instance.
   */
  public RSocketServiceTransportFactory() {
    HeadersCodec headersCodec = HeadersCodec.getInstance("application/json");
    messageCodec = new ServiceMessageCodec(headersCodec);
  }

  /**
   * Constructor with DI.
   *
   * @param headersCodec user's headers codec
   */
  public RSocketServiceTransportFactory(HeadersCodec headersCodec) {
    this.messageCodec = new ServiceMessageCodec(headersCodec);
  }

  /**
   * Fabric method for client transport.
   *
   * @param resources service transport resources
   * @return client's transport
   */
  @Override
  public RSocketClientTransport clientTransport(RSocketTransportResources resources) {
    return new RSocketClientTransport(
        messageCodec,
        resources
            .workerPool()
            .map(DelegatedLoopResources::newClientLoopResources)
            .orElse(defaultLoopResources()));
  }

  /**
   * Fabric method for server transport.
   *
   * @param resources service transport resources
   * @return server's transport
   */
  @Override
  public RSocketServerTransport serverTransport(RSocketTransportResources resources) {
    return new RSocketServerTransport(
        messageCodec,
        resources
            .workerPool()
            .map(DelegatedLoopResources::newServerLoopResources)
            .orElse(defaultLoopResources()));
  }

  private LoopResources defaultLoopResources() {
    return LoopResources.create("scalecube", 1, Runtime.getRuntime().availableProcessors(), true);
  }
}
