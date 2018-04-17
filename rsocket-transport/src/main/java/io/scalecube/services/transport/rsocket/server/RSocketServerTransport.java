package io.scalecube.services.transport.rsocket.server;

import io.scalecube.services.Services;
import io.scalecube.services.transport.rsocket.PayloadCodec;
import io.scalecube.services.transport.server.api.ServerTransport;
import io.scalecube.transport.Address;

import reactor.core.Disposable;

public class RSocketServerTransport implements ServerTransport {

  private Services services;
  private Disposable disposable;
  private PayloadCodec payloadCodec;

  public RSocketServerTransport(PayloadCodec payloadCodec) {
    this.payloadCodec = payloadCodec;
  }

  @Override
  public ServerTransport services(Services services) {
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
