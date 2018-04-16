package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.ServiceInstance;
import io.scalecube.services.transport.server.api.ServerTransport;
import io.scalecube.transport.Address;

import java.util.Collection;

import reactor.core.Disposable;

public class RSocketServerTransport implements ServerTransport {

  private Collection<ServiceInstance> services;
  private Disposable disposable;

  @Override
  public ServerTransport services(Collection<ServiceInstance> services) {
    this.services = services;
    return this;
  }

  @Override
  public Address bindAwait(int port) {
    this.disposable = RSocketServerFactory.create(port)
        .start()
        .subscribe();
    return Address.create("localhost", 7000);
  }

  @Override
  public void stop() {
    disposable.dispose();
  }
}
