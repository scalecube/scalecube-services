package io.scalecube.services.transport;

import io.scalecube.services.ServiceLoaderUtil;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.server.api.ServerTransport;

import java.util.concurrent.ExecutorService;

public interface ServiceTransport {

  ClientTransport getClientTransport();

  ServerTransport getServerTransport();

  ExecutorService getExecutorService();

  static ServiceTransport getTransport() {
    return ServiceLoaderUtil.findFirstMatched(ServiceTransport.class)
        .orElseThrow(() -> new IllegalStateException("ServiceTransport not configured"));
  }

}
