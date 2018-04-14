package io.scalecube.services.transport;

import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.server.api.ServiceTransport;

import java.util.ServiceLoader;

public interface TransportFactory {

  public static ClientTransport getClientTransport() {
    ServiceLoader<ClientTransport> loader = ServiceLoader.load(ClientTransport.class);
    return loader.iterator().next();
  }
  
  public static ServiceTransport getServiceTransport() {
    ServiceLoader<ServiceTransport> loader = ServiceLoader.load(ServiceTransport.class);
    return loader.iterator().next();
  }
}
