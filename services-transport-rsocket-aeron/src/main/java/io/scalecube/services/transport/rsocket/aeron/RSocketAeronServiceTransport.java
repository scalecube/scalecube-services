package io.scalecube.services.transport.rsocket.aeron;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import io.scalecube.services.transport.api.ServiceTransport;
import io.scalecube.services.transport.api.WorkerThreadChooser;
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

  private static final ThreadFactory WORKER_THREAD_FACTORY =
      new DefaultThreadFactory("rsocket-aeron-worker", true);

  private static final String DEFAULT_HEADERS_FORMAT = "application/json";

  private final AeronResources aeronResources = AeronResources.start(); // todo ?

  @Override
  public boolean isNativeSupported() {
    return true; // todo ?
  }

  @Override
  public ClientTransport getClientTransport(Executor workerThreadPool) {
    return new RSocketAeronClientTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)), aeronResources);
  }

  @Override
  public ServerTransport getServerTransport(Executor workerThreadPool) {
    return new RSocketAeronServerTransport(
        new ServiceMessageCodec(HeadersCodec.getInstance(DEFAULT_HEADERS_FORMAT)), aeronResources);
  }

  @Override
  public Executor getWorkerThreadPool(int numOfThreads, WorkerThreadChooser ignore) {
    return Executors.newFixedThreadPool(numOfThreads, WORKER_THREAD_FACTORY); // todo ?
  }

  @Override
  public Mono shutdown(Executor workerThreadPool) {
    return Mono.fromRunnable(
        () -> {
          if (workerThreadPool != null) {
            ((ExecutorService) workerThreadPool).shutdown();
          }
        });
  }
}
