package io.scalecube.services.transport.rsocket.client;

import io.scalecube.services.codec.ServiceMessageCodec;
import io.scalecube.services.transport.client.api.ClientChannel;
import io.scalecube.services.transport.client.api.ClientTransport;
import io.scalecube.transport.Address;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpClient;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final Map<Address, Mono<RSocket>> rSockets = new ConcurrentHashMap<>();

  private final ServiceMessageCodec codec;

  public RSocketClientTransport(ServiceMessageCodec codec) {
    this.codec = codec;
  }

  @Override
  public ClientChannel create(Address address) {
    final Map<Address, Mono<RSocket>> monoMap = rSockets; // keep reference for threadsafety
    Mono<RSocket> rSocket = monoMap.computeIfAbsent(address, address1 -> connect(address1, monoMap));
    return new RSocketServiceClientAdapter(rSocket, codec);
  }

  private static Mono<RSocket> connect(Address address, Map<Address, Mono<RSocket>> monoMap) {
    TcpClient tcpClient =
        TcpClient.create(options -> options.disablePool()
            .host(address.host())
            .port(address.port()));

    TcpClientTransport tcpClientTransport =
        TcpClientTransport.create(tcpClient);

    Mono<RSocket> rSocketMono =
        RSocketFactory.connect().transport(tcpClientTransport).start();

    return rSocketMono
        .doOnSuccess(rSocket -> {
          LOGGER.info("Connected successfully on {}", address);
          // setup shutdown hook
          rSocket.onClose().doOnTerminate(() -> {
            monoMap.remove(address);
            LOGGER.info("Connection closed on {} and removed from the pool", address);
          }).subscribe();
        })
        .doOnError(throwable -> {
          LOGGER.warn("Connect failed on {}, cause: {}", address, throwable);
          monoMap.remove(address);
        })
        .cache();
  }
}
