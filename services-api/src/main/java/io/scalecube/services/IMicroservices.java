package io.scalecube.services;

import io.scalecube.net.Address;
import io.scalecube.services.discovery.api.ServiceDiscovery;

public interface IMicroservices {

    ServiceCall call();

    Address serviceAddress();

    ServiceDiscovery discovery();

}
