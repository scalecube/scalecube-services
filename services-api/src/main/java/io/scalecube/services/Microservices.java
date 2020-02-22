package io.scalecube.services;

import io.scalecube.net.Address;
import io.scalecube.services.discovery.api.ServiceDiscovery;

public interface Microservices {

  ServiceCall call();

  Address serviceAddress();

  ServiceDiscovery discovery();
}
