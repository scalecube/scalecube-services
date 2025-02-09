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
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final Map<AddressKey, Mono<RSocket>> rsocketMap = new ConcurrentHashMap<>();

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
    final Map<AddressKey, Mono<RSocket>> monoMap = rsocketMap;

    Mono<RSocket> mono =
        monoMap.computeIfAbsent(
            new AddressKey(serviceReference.address(), serviceReference.qualifier()),
            key ->
                getCredentials(serviceReference)
                    .flatMap(credentials -> connect(key, credentials, monoMap))
                    .cacheInvalidateIf(RSocket::isDisposed)
                    .doOnError(ex -> monoMap.remove(key)));

    return new RSocketClientChannel(mono, new ServiceMessageCodec(headersCodec, dataCodecs));
  }

  private Mono<Map<String, String>> getCredentials(ServiceReference serviceReference) {
    return Mono.defer(
        () -> {
          if (credentialsSupplier == null) {
            return Mono.just(Collections.emptyMap());
          }

          if (!serviceReference.isSecured()) {
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
      AddressKey addressKey,
      Map<String, String> credentials,
      Map<AddressKey, Mono<RSocket>> monoMap) {
    return RSocketConnector.create()
        .payloadDecoder(PayloadDecoder.DEFAULT)
        .setupPayload(encodeConnectionSetup(new ConnectionSetup(credentials)))
        .connect(() -> clientTransportFactory.clientTransport(addressKey.address))
        .doOnSuccess(
            rsocket -> {
              LOGGER.debug("[rsocket][client] Connected successfully ({})", addressKey);
              // setup shutdown hook
              rsocket
                  .onClose()
                  .doFinally(
                      s -> {
                        monoMap.remove(addressKey);
                        LOGGER.debug("[rsocket][client] Connection closed ({})", addressKey);
                      })
                  .doOnError(
                      th ->
                          LOGGER.warn(
                              "[rsocket][client][onClose] Exception occurred ({}), cause: {}",
                              addressKey,
                              th.toString()))
                  .subscribe();
            })
        .doOnError(
            th ->
                LOGGER.warn(
                    "[rsocket][client] Failed to connect ({}), cause: {}",
                    addressKey,
                    th.toString()));
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
    if (th instanceof ServiceException e) {
      return new UnauthorizedException(e.errorCode(), e.getMessage());
    } else {
      return new UnauthorizedException(th);
    }
  }

  @Override
  public void close() {
    rsocketMap.forEach(
        (address, socketMono) ->
            socketMono.subscribe(
                RSocket::dispose,
                throwable -> {
                  // no-op
                }));
    rsocketMap.clear();
  }

  private record AddressKey(Address address, String qualifier) {

    @Override
    public String toString() {
      return new StringJoiner(", ", AddressKey.class.getSimpleName() + "[", "]")
          .add("address=" + address)
          .add("qualifier='" + qualifier + "'")
          .toString();
    }
  }
}
