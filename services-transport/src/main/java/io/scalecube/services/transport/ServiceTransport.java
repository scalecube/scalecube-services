package io.scalecube.services.transport;

import io.scalecube.services.codecs.api.ServiceMessageCodec;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.server.api.ServerTransport;

import java.util.Map;
import java.util.ServiceLoader;

public interface ServiceTransport {

  ClientTransport getClientTransport();

  ServerTransport getServerTransport();

  Map<String, ? extends ServiceMessageCodec> getMessageCodecs();

  static ServiceTransport getTransport() {
    return getNext(ServiceLoader.load(ServiceTransport.class));
  }

  static <T> T getNext(ServiceLoader<T> loader) {
    if (loader.iterator().hasNext()) {
      return loader.iterator().next();
    } else {
      return null;
    }
  }

}
