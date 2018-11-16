package io.scalecube.services.transport.rsocket;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.transport.api.Address;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;

/** RSocket client transport implementation. */
public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final ThreadLocal<Map<Address, Mono<RSocket>>> rsockets =
      ThreadLocal.withInitial(ConcurrentHashMap::new);

  private final ServiceMessageCodec codec;
  private final LoopResources loopResources;

  /**
   * Constructor for this transport.
   *
   * @param codec message codec
   * @param loopResources client loop resources
   */
  public RSocketClientTransport(ServiceMessageCodec codec, LoopResources loopResources) {
    this.codec = codec;
    this.loopResources = loopResources;
  }

  @Override
  public ClientChannel create(Address address) {
    final Map<Address, Mono<RSocket>> monoMap = rsockets.get(); // keep reference for threadsafety
    Mono<RSocket> rsocket =
        monoMap.computeIfAbsent(address, address1 -> connect(address1, monoMap));
    return new RSocketServiceClientAdapter(rsocket, codec);
  }

  private Mono<RSocket> connect(Address address, Map<Address, Mono<RSocket>> monoMap) {
    TcpClient tcpClient =
        TcpClient.newConnection() // create non-pooled
            .runOn(loopResources)
            .host(address.host())
            .port(address.port());

    Mono<RSocket> rsocketMono =
        RSocketFactory.connect()
            .frameDecoder(
                frame ->
                    ByteBufPayload.create(
                        frame.sliceData().retain(), frame.sliceMetadata().retain()))
            .transport(() -> TcpClientTransport.create(tcpClient))
            .start();

    return rsocketMono
        .doOnSuccess(
            rsocket -> {
              LOGGER.info("Connected successfully on {}", address);
              // setup shutdown hook
              rsocket
                  .onClose()
                  .doOnTerminate(
                      () -> {
                        monoMap.remove(address);
                        LOGGER.info("Connection closed on {} and removed from the pool", address);
                      })
                  .subscribe(null, th -> LOGGER.warn("Exception on closing rsocket: {}", th));
            })
        .doOnError(
            throwable -> {
              LOGGER.warn("Connect failed on {}, cause: {}", address, throwable);
              monoMap.remove(address);
            })
        .cache();
  }
}
