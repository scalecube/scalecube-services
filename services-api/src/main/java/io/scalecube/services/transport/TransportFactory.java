package io.scalecube.services.transport;

import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.server.api.ServiceTransport;

import java.util.ServiceLoader;

public class TransportFactory {

  public static ClientTransport getClientTransport() {
    return getNext(ServiceLoader.load(ClientTransport.class));
  }
  
  public static ServiceTransport getServiceTransport() {
    return getNext(ServiceLoader.load(ServiceTransport.class));
  }

  private static <T> T getNext(ServiceLoader<T> loader) {
    if(loader.iterator().hasNext()) {
      return loader.iterator().next();
    } else {
      return null;
    }
  }
}
