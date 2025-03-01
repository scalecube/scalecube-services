package io.scalecube.services.gateway;

import io.scalecube.services.RequestContext;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.gateway.AuthRegistry.AllowedUser;
import java.util.stream.IntStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SecuredServiceImpl implements SecuredService {

  private final AuthRegistry authRegistry;

  public SecuredServiceImpl(AuthRegistry authRegistry) {
    this.authRegistry = authRegistry;
  }

  @Override
  public Mono<String> createSession(ServiceMessage request) {
    String sessionId = request.header(AuthRegistry.SESSION_ID);
    if (sessionId == null) {
      throw new BadRequestException("sessionId is not present in request");
    }
    String req = request.data();
    if (!authRegistry.addAuth(Long.parseLong(sessionId), req)) {
      throw new UnauthorizedException("Authentication failed");
    }
    return Mono.just(req);
  }

  @Override
  public Mono<String> requestOne(String req) {
    return RequestContext.deferContextual()
        .map(
            context -> {
              if (!context.hasPrincipal()) {
                throw new ForbiddenException("Not allowed");
              }
              final var principal = (AllowedUser) context.principal();
              return principal.username() + "@" + req;
            });
  }

  @Override
  public Flux<String> requestMany(Integer times) {
    return RequestContext.deferContextual()
        .flatMapMany(
            context -> {
              if (!context.hasPrincipal()) {
                throw new ForbiddenException("Not allowed");
              }
              if (times <= 0) {
                return Flux.empty();
              }
              return Flux.fromStream(IntStream.range(0, times).mapToObj(String::valueOf));
            });
  }
}
