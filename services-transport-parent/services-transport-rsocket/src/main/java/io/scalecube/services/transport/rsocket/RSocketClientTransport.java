package io.scalecube.services.transport.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.exceptions.MessageCodecException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServiceTransport.CredentialsSupplier;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final Map<Address, Mono<RSocket>> rsockets = new ConcurrentHashMap<>();

  private final CredentialsSupplier credentialsSupplier;
  private final ConnectionSetupCodec connectionSetupCodec;
  private final HeadersCodec headersCodec;
  private final Collection<DataCodec> dataCodecs;
  private final RSocketClientTransportFactory clientTransportFactory;

  /**
   * Constructor for this transport.
   *
   * @param credentialsSupplier credentialsSupplier
   * @param connectionSetupCodec connectionSetupCodec
   * @param headersCodec headersCodec
   * @param dataCodecs dataCodecs
   * @param clientTransportFactory clientTransportFactory
   */
  public RSocketClientTransport(
      CredentialsSupplier credentialsSupplier,
      ConnectionSetupCodec connectionSetupCodec,
      HeadersCodec headersCodec,
      Collection<DataCodec> dataCodecs,
      RSocketClientTransportFactory clientTransportFactory) {
    this.credentialsSupplier = credentialsSupplier;
    this.connectionSetupCodec = connectionSetupCodec;
    this.headersCodec = headersCodec;
    this.dataCodecs = dataCodecs;
    this.clientTransportFactory = clientTransportFactory;
  }

  @Override
  public ClientChannel create(ServiceReference serviceReference) {
    final Map<Address, Mono<RSocket>> monoMap = this.rsockets; // keep reference for threadsafety
    final Address address = serviceReference.address();
    Mono<RSocket> mono =
        monoMap.computeIfAbsent(
            address,
            key ->
                getCredentials(serviceReference)
                    .flatMap(creds -> connect(key, creds, monoMap))
                    .cache()
                    .doOnError(ex -> monoMap.remove(key)));
    return new RSocketClientChannel(mono, new ServiceMessageCodec(headersCodec, dataCodecs));
  }

  private Mono<Map<String, String>> getCredentials(ServiceReference serviceReference) {
    return Mono.defer(
        () -> {
          if (credentialsSupplier == null) {
            return Mono.just(Collections.emptyMap());
          }
          return credentialsSupplier
              .apply(serviceReference)
              .switchIfEmpty(Mono.just(Collections.emptyMap()))
              .doOnError(
                  ex ->
                      LOGGER.error(
                          "[credentialsSupplier] "
                              + "Failed to get credentials for service: {}, cause: {}",
                          serviceReference,
                          ex.toString()))
              .onErrorMap(this::toUnauthorizedException);
        });
  }

  private Mono<RSocket> connect(
      Address address, Map<String, String> creds, Map<Address, Mono<RSocket>> monoMap) {
    return RSocketConnector.create()
        .payloadDecoder(PayloadDecoder.DEFAULT)
        .setupPayload(encodeConnectionSetup(new ConnectionSetup(creds)))
        .connect(() -> clientTransportFactory.clientTransport(address))
        .doOnSuccess(
            rsocket -> {
              LOGGER.debug("[rsocket][client][{}] Connected successfully", address);
              // setup shutdown hook
              rsocket
                  .onClose()
                  .doFinally(
                      s -> {
                        monoMap.remove(address);
                        LOGGER.debug("[rsocket][client][{}] Connection closed", address);
                      })
                  .doOnError(
                      th ->
                          LOGGER.warn(
                              "[rsocket][client][{}][onClose] Exception occurred: {}",
                              address,
                              th.toString()))
                  .subscribe();
            })
        .doOnError(
            th ->
                LOGGER.warn(
                    "[rsocket][client][{}] Failed to connect, cause: {}", address, th.toString()));
  }

  private Payload encodeConnectionSetup(ConnectionSetup connectionSetup) {
    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    try {
      connectionSetupCodec.encode(new ByteBufOutputStream(byteBuf), connectionSetup);
    } catch (Throwable ex) {
      ReferenceCountUtil.safestRelease(byteBuf);
      LOGGER.error(
          "Failed to encode connectionSetup: {}, cause: {}", connectionSetup, ex.toString());
      throw new MessageCodecException("Failed to encode ConnectionSetup", ex);
    }
    return ByteBufPayload.create(byteBuf);
  }

  private UnauthorizedException toUnauthorizedException(Throwable th) {
    if (th instanceof ServiceException) {
      ServiceException e = (ServiceException) th;
      return new UnauthorizedException(e.errorCode(), e.getMessage());
    } else {
      return new UnauthorizedException(th);
    }
  }

  @Override
  public void close() {
    rsockets.forEach(
        (address, socketMono) ->
            socketMono.subscribe(
                RSocket::dispose,
                throwable -> {
                  // no-op
                }));
    rsockets.clear();
  }
}
