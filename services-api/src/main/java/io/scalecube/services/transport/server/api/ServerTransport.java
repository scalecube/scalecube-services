package io.scalecube.services.transport.server.api;

import io.scalecube.services.ServiceInstance;
import io.scalecube.transport.Address;

import java.util.Collection;

public interface ServerTransport {

  ServerTransport services(Collection<ServiceInstance> services);

  Address bindAwait();
  
}

