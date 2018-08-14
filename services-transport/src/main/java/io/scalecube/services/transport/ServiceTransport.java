package io.scalecube.services.transport;

import io.scalecube.services.ServiceLoaderUtil;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.server.api.ServerTransport;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutorService;

public interface ServiceTransport {

  static ServiceTransport getTransport() {
    return ServiceLoaderUtil.findFirst(ServiceTransport.class)
        .orElseThrow(() -> new IllegalStateException("ServiceTransport not configured"));
  }

  ClientTransport getClientTransport();

  ServerTransport getServerTransport();

  ExecutorService getExecutorService();

  Mono<Void> shutdown();

}
