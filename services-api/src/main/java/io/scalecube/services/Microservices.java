package io.scalecube.services;

import io.scalecube.net.Address;
import io.scalecube.services.discovery.api.ServiceDiscovery;

public interface Microservices {

  String id();

  ServiceCall call();

  Address serviceAddress();

  ServiceDiscovery discovery();
}
