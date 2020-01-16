package io.scalecube.services.transport.rsocket;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.scalecube.net.Address;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.ServiceMessageCodec;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final ThreadLocal<Map<Address, Mono<RSocket>>> rsockets =
      ThreadLocal.withInitial(ConcurrentHashMap::new);

  private final ServiceMessageCodec codec;
  private final TcpClient tcpClient;

  /**
   * Constructor for this transport.
   *
   * @param codec message codec
   * @param tcpClient tcp client
   */
  public RSocketClientTransport(ServiceMessageCodec codec, TcpClient tcpClient) {
    this.codec = codec;
    this.tcpClient = tcpClient;
  }

  @Override
  public ClientChannel create(Address address) {
    final Map<Address, Mono<RSocket>> monoMap = rsockets.get(); // keep reference for threadsafety
    Mono<RSocket> rsocket =
        monoMap.computeIfAbsent(address, address1 -> connect(address1, monoMap));
    return new RSocketClientChannel(rsocket, codec);
  }

  private Mono<RSocket> connect(Address address, Map<Address, Mono<RSocket>> monoMap) {
    TcpClient tcpClient = this.tcpClient.host(address.host()).port(address.port());

    Mono<RSocket> rsocketMono =
        RSocketFactory.connect()
            .frameDecoder(PayloadDecoder.DEFAULT)
            .errorConsumer(
                th -> LOGGER.warn("Exception occurred at rsocket client transport: " + th))
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
                        LOGGER.info("Connection closed on {}", address);
                      })
                  .subscribe(
                      null, th -> LOGGER.warn("Exception on closing rsocket: {}", th.toString()));
            })
        .doOnError(
            th -> {
              LOGGER.warn("Connect failed on {}, cause: {}", address, th.toString());
              monoMap.remove(address);
            })
        .cache();
  }

  @Override
  public String toString() {
    return "RSocketClientTransport{" + "codec=" + codec + ", tcpClient=" + tcpClient + '}';
  }
}
