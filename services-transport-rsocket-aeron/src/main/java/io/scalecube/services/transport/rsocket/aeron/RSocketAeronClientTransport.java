package io.scalecube.services.transport.rsocket.aeron;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.reactor.aeron.AeronClientTransport;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.transport.api.Address;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.AeronClient;
import reactor.aeron.AeronResources;
import reactor.core.publisher.Mono;

/** RSocket Aeron client transport implementation. */
public class RSocketAeronClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketAeronClientTransport.class);

  private final ThreadLocal<Map<Address, Mono<RSocket>>> rsockets =
      ThreadLocal.withInitial(ConcurrentHashMap::new);

  private final ServiceMessageCodec codec;
  private final AeronResources aeronResources;

  /**
   * Constructor for this transport.
   *
   * @param codec message codec
   * @param aeronResources client aeron resources
   */
  public RSocketAeronClientTransport(ServiceMessageCodec codec, AeronResources aeronResources) {
    this.codec = codec;
    this.aeronResources = aeronResources;
  }

  @Override
  public ClientChannel create(Address address) {
    final Map<Address, Mono<RSocket>> monoMap = rsockets.get(); // keep reference for threadsafety
    Mono<RSocket> rsocket =
        monoMap.computeIfAbsent(address, address1 -> connect(address1, monoMap));
    return new RSocketAeronServiceClientAdapter(rsocket, codec);
  }

  private Mono<RSocket> connect(Address address, Map<Address, Mono<RSocket>> monoMap) {
    AeronClient aeronClient =
        AeronClient.create(aeronResources)
            .options(address.host(), address.port(), address.port() + 1); // todo

    Mono<RSocket> rsocketMono =
        RSocketFactory.connect()
            .frameDecoder(
                frame ->
                    ByteBufPayload.create(
                        frame.sliceData().retain(), frame.sliceMetadata().retain()))
            .transport(() -> new AeronClientTransport(aeronClient))
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
