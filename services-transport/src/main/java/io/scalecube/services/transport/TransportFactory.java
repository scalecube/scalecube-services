package io.scalecube.services.transport;

import java.util.ServiceLoader;

public class TransportFactory {

  public static ServiceTransport getTransport() {
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
