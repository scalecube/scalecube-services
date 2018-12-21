package io.scalecube.services.transport.rsocket.aeron;

import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransport;
import java.util.Optional;
import java.util.concurrent.Executor;
import reactor.aeron.AeronResources;
import reactor.aeron.AeronResourcesConfig;
import reactor.core.publisher.Mono;

public class RSocketAeronServiceTransport implements ServiceTransport {

  private static final String DEFAULT_HEADERS_FORMAT = "application/json";

  @Override
  public ServiceTransport.Resources resources(int numOfWorkers) {
    return null;
  }

  @Override
  public ClientTransport clientTransport(ServiceTransport.Resources resources) {
    return new RSocketAeronClientTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)),
        ((Resources) resources).aeronResources);
  }

  @Override
  public ServerTransport serverTransport(ServiceTransport.Resources resources) {
    return new RSocketAeronServerTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)),
        ((Resources) resources).aeronResources);
  }

  /**
   * Aeron RSocket service transport Resources implementation. Contains internally {@link
   * AeronResources} instance.
   */
  private static class Resources implements ServiceTransport.Resources {

    private final AeronResources aeronResources;

    private Resources(int numOfWorkers) {
      aeronResources =
          AeronResources.start(
              AeronResourcesConfig //
                  .builder()
                  .numOfWorkers(numOfWorkers)
                  .build());
    }

    @Override
    public Optional<Executor> workerPool() {
      // worker pool is not exposed in aeron
      return Optional.empty();
    }

    @Override
    public Mono<Void> shutdown() {
      return Mono.defer(
          () -> {
            aeronResources.dispose();
            return aeronResources.onDispose();
          });
    }
  }
}
