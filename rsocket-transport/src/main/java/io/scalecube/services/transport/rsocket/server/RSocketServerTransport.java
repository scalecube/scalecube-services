package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.ServiceInstance;
import io.scalecube.services.transport.rsocket.PayloadCodec;
import io.scalecube.services.transport.server.api.ServerTransport;
import io.scalecube.transport.Address;
import reactor.core.Disposable;

import java.util.Collection;

public class RSocketServerTransport implements ServerTransport {

  private Collection<ServiceInstance> services;
  private Disposable disposable;
  private PayloadCodec payloadCodec;

  public RSocketServerTransport(PayloadCodec payloadCodec) {
    this.payloadCodec = payloadCodec;
  }

  @Override
  public ServerTransport services(Collection<ServiceInstance> services) {
    this.services = services;
    return this;
  }

  @Override
  public Address bindAwait(int port) {
    this.disposable = RSocketServerFactory.create(port, payloadCodec)
        .start()
        .subscribe();
    return Address.create("localhost", port);
  }

  @Override
  public void stop() {
    disposable.dispose();
  }
}
