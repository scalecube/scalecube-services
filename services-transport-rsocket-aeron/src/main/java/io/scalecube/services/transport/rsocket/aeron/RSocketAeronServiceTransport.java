package io.scalecube.services.transport.rsocket.aeron;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransport;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronResources;
import reactor.core.publisher.Mono;

public class RSocketAeronServiceTransport implements ServiceTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketAeronServiceTransport.class);

  private static final String DEFAULT_HEADERS_FORMAT = "application/json";

  @Override
  public boolean isNativeSupported() {
    return true;
  }

  @Override
  public ClientTransport getClientTransport(Executor aeronRecourcesHolder) {
    AeronResources aeronResources = ((AeronRecourcesHolder) aeronRecourcesHolder).aeronResources;
    return new RSocketAeronClientTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)), aeronResources);
  }

  @Override
  public ServerTransport getServerTransport(Executor aeronRecourcesHolder) {
    AeronResources aeronResources = ((AeronRecourcesHolder) aeronRecourcesHolder).aeronResources;
    return new RSocketAeronServerTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)), aeronResources);
  }

  @Override
  public Executor getWorkerThreadPool(int numOfThreads) {
    return new AeronRecourcesHolder(numOfThreads);
  }

  @Override
  public Mono shutdown(Executor aeronRecourcesHolder) {
    return Mono.fromRunnable(
        () -> {
          if (aeronRecourcesHolder != null) {
            ((AeronRecourcesHolder) aeronRecourcesHolder).shutdown();
          }
        });
  }

  private static class AeronRecourcesHolder implements Executor {

    private static final ThreadFactory WORKER_THREAD_FACTORY =
        new DefaultThreadFactory("rsocket-aeron-worker", true);

    private final ExecutorService workerThreadPool;
    private final AeronResources aeronResources;

    private AeronRecourcesHolder(int numOfThreads) {
      this.workerThreadPool = Executors.newFixedThreadPool(numOfThreads, WORKER_THREAD_FACTORY);
      this.aeronResources = AeronResources.start();
    }

    @Override
    public void execute(Runnable command) {
      workerThreadPool.execute(command);
    }

    private void shutdown() {
      workerThreadPool.shutdown();
      aeronResources.dispose();
      aeronResources.onDispose().block();
    }
  }
}
