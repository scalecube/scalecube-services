package io.scalecube.services.transport.server.api;

import io.scalecube.services.ServiceInstance;
import io.scalecube.transport.Address;

import java.util.Collection;

public interface ServiceTransport {

  ServiceTransport services(Collection<ServiceInstance> services);

  Address bindAwait();
  
}

