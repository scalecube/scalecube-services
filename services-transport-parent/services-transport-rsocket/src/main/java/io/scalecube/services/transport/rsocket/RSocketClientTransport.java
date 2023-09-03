package io.scalecube.services.transport.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.util.ByteBufPayload;
import io.scalecube.net.Address;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.auth.CredentialsSupplier;
import io.scalecube.services.exceptions.MessageCodecException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.utils.MaskUtil;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final ThreadLocal<Map<String, Mono<RSocket>>> connections =
      ThreadLocal.withInitial(ConcurrentHashMap::new);

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
    final String endpointId = serviceReference.endpointId();
    final Map<String, Mono<RSocket>> connections = this.connections.get();

    Mono<RSocket> promise =
        connections.computeIfAbsent(
            endpointId,
            key ->
                connect(serviceReference, connections)
                    .cache()
                    .doOnError(ex -> connections.remove(key)));

    return new RSocketClientChannel(promise, new ServiceMessageCodec(headersCodec, dataCodecs));
  }

  private Mono<RSocket> connect(
      ServiceReference serviceReference, Map<String, Mono<RSocket>> connections) {
    return Mono.defer(
        () -> {
          final String endpointId = serviceReference.endpointId();
          final List<Address> addresses = serviceReference.addresses();
          final AtomicInteger currentIndex = new AtomicInteger(0);

          return Mono.defer(
                  () -> {
                    final Address address = addresses.get(currentIndex.get());
                    return connect(serviceReference, connections, address, endpointId);
                  })
              .doOnError(ex -> currentIndex.incrementAndGet())
              .retry(addresses.size() - 1)
              .doOnError(
                  th ->
                      LOGGER.warn(
                          "Failed to connect ({}/{}), cause: {}",
                          endpointId,
                          addresses,
                          th.toString()));
        });
  }

  private Mono<RSocket> connect(
      ServiceReference serviceReference,
      Map<String, Mono<RSocket>> connections,
      Address address,
      String endpointId) {
    return getCredentials(serviceReference)
        .flatMap(
            creds ->
                RSocketConnector.create()
                    .payloadDecoder(PayloadDecoder.DEFAULT)
                    .setupPayload(encodeConnectionSetup(new ConnectionSetup(creds)))
                    .connect(() -> clientTransportFactory.clientTransport(address)))
        .doOnSuccess(
            rsocket -> {
              LOGGER.debug("[{}] Connected successfully", address);
              // Setup shutdown hook
              rsocket
                  .onClose()
                  .doFinally(
                      s -> {
                        connections.remove(endpointId);
                        LOGGER.debug("[{}] Connection closed", address);
                      })
                  .doOnError(
                      th -> LOGGER.warn("[{}] Exception on close: {}", address, th.toString()))
                  .subscribe();
            });
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
              .doOnSuccess(
                  creds ->
                      LOGGER.debug(
                          "[credentialsSupplier] Got credentials ({}) for service: {}",
                          MaskUtil.mask(creds),
                          serviceReference))
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
}
