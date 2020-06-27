package io.scalecube.services.transport.rsocket;

import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.scalecube.net.Address;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final ThreadLocal<Map<Address, Mono<RSocket>>> rsockets =
      ThreadLocal.withInitial(ConcurrentHashMap::new);

  private final ServiceMessageCodec messageCodec;
  private final RSocketClientTransportFactory clientTransportFactory;

  /**
   * Constructor for this transport.
   *
   * @param messageCodec messageCodec
   * @param clientTransportFactory clientTransportFactory
   */
  public RSocketClientTransport(
      ServiceMessageCodec messageCodec, RSocketClientTransportFactory clientTransportFactory) {
    this.messageCodec = messageCodec;
    this.clientTransportFactory = clientTransportFactory;
  }

  @Override
  public ClientChannel create(Address address) {
    final Map<Address, Mono<RSocket>> monoMap = rsockets.get(); // keep reference for threadsafety
    Mono<RSocket> rsocket =
        monoMap.computeIfAbsent(address, address1 -> connect(address1, monoMap));
    return new RSocketClientChannel(rsocket, messageCodec);
  }

  private Mono<RSocket> connect(Address address, Map<Address, Mono<RSocket>> monoMap) {
    return RSocketConnector.create()
        .payloadDecoder(PayloadDecoder.DEFAULT)
        .connect(() -> clientTransportFactory.clientTransport(address))
        .doOnSuccess(
            rsocket -> {
              LOGGER.debug("[rsocket][client] Connected successfully on {}", address);
              // setup shutdown hook
              rsocket
                  .onClose()
                  .doFinally(
                      s -> {
                        monoMap.remove(address);
                        LOGGER.debug("[rsocket][client] Connection closed on {}", address);
                      })
                  .doOnError(
                      th ->
                          LOGGER.warn(
                              "[rsocket][client][onClose] Exception occurred: {}", th.toString()))
                  .subscribe();
            })
        .doOnError(
            th -> {
              LOGGER.warn(
                  "[rsocket][client] Failed to connect on {}, cause: {}", address, th.toString());
              monoMap.remove(address);
            })
        .cache();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RSocketClientTransport.class.getSimpleName() + "[", "]")
        .add("messageCodec=" + messageCodec)
        .toString();
  }
}
