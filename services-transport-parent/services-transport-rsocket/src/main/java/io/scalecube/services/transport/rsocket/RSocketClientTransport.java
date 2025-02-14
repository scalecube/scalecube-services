package io.scalecube.services.transport.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ClientChannel;
import io.scalecube.services.transport.api.ClientTransport;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketClientTransport implements ClientTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketClientTransport.class);

  private final Map<Destination, Mono<RSocket>> rsockets = new ConcurrentHashMap<>();

  private final HeadersCodec headersCodec;
  private final Collection<DataCodec> dataCodecs;
  private final RSocketClientTransportFactory clientTransportFactory;
  private final CredentialsSupplier credentialsSupplier;
  private final List<String> allowedRoles;

  /**
   * Constructor for this transport.
   *
   * @param headersCodec headersCodec
   * @param dataCodecs dataCodecs
   * @param clientTransportFactory clientTransportFactory
   * @param credentialsSupplier credentialsSupplier
   * @param allowedRoles allowedRoles
   */
  public RSocketClientTransport(
      HeadersCodec headersCodec,
      Collection<DataCodec> dataCodecs,
      RSocketClientTransportFactory clientTransportFactory,
      CredentialsSupplier credentialsSupplier,
      List<String> allowedRoles) {
    this.headersCodec = headersCodec;
    this.dataCodecs = dataCodecs;
    this.clientTransportFactory = clientTransportFactory;
    this.credentialsSupplier = credentialsSupplier;
    this.allowedRoles = allowedRoles;
  }

  @Override
  public ClientChannel create(ServiceReference serviceReference) {
    final var monoMap = rsockets;
    final var address = serviceReference.address();
    final var serviceRole = selectServiceRole(serviceReference);

    final var mono =
        monoMap.computeIfAbsent(
            new Destination(address, serviceRole),
            key ->
                connect(key, serviceReference, monoMap)
                    .cacheInvalidateIf(RSocket::isDisposed)
                    .doOnError(ex -> monoMap.remove(key)));

    return new RSocketClientChannel(mono, new ServiceMessageCodec(headersCodec, dataCodecs));
  }

  private String selectServiceRole(ServiceReference serviceReference) {
    if (credentialsSupplier == null
        || !serviceReference.isSecured()
        || !serviceReference.hasAllowedRoles()) {
      return null;
    }

    if (allowedRoles == null || allowedRoles.isEmpty()) {
      return serviceReference.allowedRoles().get(0);
    }

    for (var allowedRole : allowedRoles) {
      if (serviceReference.allowedRoles().contains(allowedRole)) {
        return allowedRole;
      }
    }

    throw new ForbiddenException("Forbidden");
  }

  private Mono<RSocket> connect(
      Destination destination,
      ServiceReference serviceReference,
      Map<Destination, Mono<RSocket>> monoMap) {
    return RSocketConnector.create()
        .setupPayload(Mono.defer(() -> getCredentials(serviceReference, destination.role())))
        .connect(() -> clientTransportFactory.clientTransport(destination.address()))
        .doOnSuccess(
            rsocket -> {
              LOGGER.debug("[rsocket][client][{}] Connected successfully", destination.address());
              // setup shutdown hook
              rsocket
                  .onClose()
                  .doFinally(
                      s -> {
                        monoMap.remove(destination);
                        LOGGER.debug(
                            "[rsocket][client][{}] Connection closed", destination.address());
                      })
                  .doOnError(
                      th ->
                          LOGGER.warn(
                              "[rsocket][client][{}][onClose] Exception occurred: {}",
                              destination.address(),
                              th.toString()))
                  .subscribe();
            })
        .doOnError(
            th ->
                LOGGER.warn(
                    "[rsocket][client][{}] Failed to connect, cause: {}",
                    destination.address(),
                    th.toString()));
  }

  private Mono<Payload> getCredentials(ServiceReference serviceReference, String serviceRole) {
    if (credentialsSupplier == null || !serviceReference.isSecured()) {
      return Mono.just(EmptyPayload.INSTANCE);
    }
    return credentialsSupplier
        .credentials(serviceReference, serviceRole)
        .map(DefaultPayload::create)
        .onErrorMap(
            th -> {
              if (th instanceof ServiceException e) {
                return new UnauthorizedException(e.errorCode(), e.getMessage());
              } else {
                return new UnauthorizedException(th);
              }
            });
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

  private record Destination(Address address, String role) {}
}
