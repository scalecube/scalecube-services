package io.scalecube.services.transport;

import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.services.transport.server.api.ServerTransport;

import java.util.Collection;
import java.util.Map;

public interface ServiceTransport {

  ClientTransport getClientTransport();
  
  ServerTransport getServerTransport();

  Map<String, ? extends ServiceMessageCodec> getMessageCodec();
 
}
