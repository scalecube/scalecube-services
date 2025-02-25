package io.scalecube.services.transport.rsocket;

import io.netty.buffer.ByteBuf;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.scalecube.services.RequestContext;
import io.scalecube.services.exceptions.ServiceException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.ServerTransport.Authenticator;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketServiceAcceptor implements SocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RSocketServiceAcceptor.class);

  private final HeadersCodec headersCodec;
  private final Collection<DataCodec> dataCodecs;
  private final Authenticator authenticator;
  private final ServiceRegistry serviceRegistry;

  public RSocketServiceAcceptor(
      HeadersCodec headersCodec,
      Collection<DataCodec> dataCodecs,
      Authenticator authenticator,
      ServiceRegistry serviceRegistry) {
    this.headersCodec = headersCodec;
    this.dataCodecs = dataCodecs;
    this.authenticator = authenticator;
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket rsocket) {
    return Mono.defer(() -> authenticate(setupPayload.data())).map(this::newRSocket);
  }

  private Mono<Object> authenticate(ByteBuf connectionSetup) {
    if (authenticator == null) {
      return Mono.just(RequestContext.NULL_PRINCIPAL);
    }

    final var credentials = new byte[connectionSetup.readableBytes()];
    connectionSetup.getBytes(connectionSetup.readerIndex(), credentials);

    return authenticator
        .authenticate(credentials)
        .switchIfEmpty(Mono.just(RequestContext.NULL_PRINCIPAL))
        .doOnSuccess(p -> LOGGER.debug("Authenticated successfully, principal: {}", p))
        .doOnError(ex -> LOGGER.error("Failed to authenticate, cause: {}", ex.toString()))
        .onErrorMap(RSocketServiceAcceptor::toUnauthorizedException);
  }

  private RSocket newRSocket(Object principal) {
    return new RSocketImpl(
        principal, new ServiceMessageCodec(headersCodec, dataCodecs), serviceRegistry);
  }

  private static UnauthorizedException toUnauthorizedException(Throwable th) {
    if (th instanceof ServiceException ex) {
      return new UnauthorizedException(ex.errorCode(), ex.getMessage());
    } else {
      return new UnauthorizedException(th);
    }
  }
}
