package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceLoaderUtil;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.transport.Address;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServiceDiscovery {
  public static final String SERVICE_METADATA = "service";
  
  Address address();

  ServiceEndpoint endpoint();

  Mono<String> spreadGossip(ServiceMessage gossip);
  
  static ServiceDiscovery getDiscovery() {
    ServiceDiscovery discovery = ServiceLoaderUtil.findFirstMatched(ServiceDiscovery.class)
        .orElseThrow(() -> new IllegalStateException("ServiceDiscovery not configured"));
    
    return discovery;
  }

  Mono<ServiceDiscovery> start(DiscoveryConfig discoveryConfig);

  Mono<Void> shutdown();  
  
  Flux<DiscoveryEvent> listen();

  Flux<ServiceMessage> listenGossip();
}
