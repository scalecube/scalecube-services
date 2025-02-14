package io.scalecube.services.transport.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final Map<Address, Mono<RSocket>> rsockets = new ConcurrentHashMap<>();

  private final CredentialsSupplier credentialsSupplier;
  private final HeadersCodec headersCodec;
  private final Collection<DataCodec> dataCodecs;
  private final RSocketClientTransportFactory clientTransportFactory;

  /**
   * Constructor for this transport.
   *
   * @param credentialsSupplier credentialsSupplier
   * @param headersCodec headersCodec
   * @param dataCodecs dataCodecs
   * @param clientTransportFactory clientTransportFactory
   */
  public RSocketClientTransport(
      CredentialsSupplier credentialsSupplier,
      HeadersCodec headersCodec,
      Collection<DataCodec> dataCodecs,
      RSocketClientTransportFactory clientTransportFactory) {
    this.credentialsSupplier = credentialsSupplier;
    this.headersCodec = headersCodec;
    this.dataCodecs = dataCodecs;
    this.clientTransportFactory = clientTransportFactory;
  }

  @Override
  public ClientChannel create(ServiceReference serviceReference) {
    final var monoMap = rsockets;
    final var address = serviceReference.address();

    final var mono =
        monoMap.computeIfAbsent(
            address,
            key ->
                connect(key, serviceReference, monoMap)
                    .cacheInvalidateIf(RSocket::isDisposed)
                    .doOnError(ex -> monoMap.remove(key)));

    return new RSocketClientChannel(mono, new ServiceMessageCodec(headersCodec, dataCodecs));
  }

  private Mono<RSocket> connect(
      Address address, ServiceReference serviceReference, Map<Address, Mono<RSocket>> monoMap) {
    return RSocketConnector.create()
        .setupPayload(getCredentials(serviceReference))
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

  private Mono<Payload> getCredentials(ServiceReference serviceReference) {
    return Mono.defer(
        () -> {
          if (credentialsSupplier == null || !serviceReference.isSecured()) {
            return Mono.just(EmptyPayload.INSTANCE);
          }

          return credentialsSupplier
              .credentials(serviceReference)
              .map(DefaultPayload::create)
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

  private UnauthorizedException toUnauthorizedException(Throwable th) {
    if (th instanceof ServiceException e) {
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
