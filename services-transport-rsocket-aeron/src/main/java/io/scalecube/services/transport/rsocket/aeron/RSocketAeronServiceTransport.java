package io.scalecube.services.transport.rsocket.aeron;

import io.aeron.driver.ThreadingMode;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransport;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import reactor.aeron.AeronResources;
import reactor.core.publisher.Mono;

public class RSocketAeronServiceTransport implements ServiceTransport {

  private static final String DEFAULT_HEADERS_FORMAT = "application/json";

  @Override
  public ServiceTransport.Resources resources(int numOfWorkers) {
    return new Resources(numOfWorkers);
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
      Supplier<IdleStrategy> idleStrategy =
          () -> new BackoffIdleStrategy(10, 20, 100, TimeUnit.MILLISECONDS.toNanos(1));
      aeronResources =
          new AeronResources()
              .useTmpDir()
              .singleWorker()
              .aeron(ctx -> ctx.idleStrategy(idleStrategy.get()))
              .media(
                  ctx ->
                      ctx.threadingMode(ThreadingMode.DEDICATED)
                          .sharedIdleStrategy(idleStrategy.get())
                          .sharedNetworkIdleStrategy(idleStrategy.get())
                          .conductorIdleStrategy(idleStrategy.get())
                          .receiverIdleStrategy(idleStrategy.get())
                          .senderIdleStrategy(idleStrategy.get())
                          .termBufferSparseFile(false))
              .workerIdleStrategySupplier(idleStrategy)
              .singleWorker()
              .start()
              .block();
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
