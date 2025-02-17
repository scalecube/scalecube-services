package io.scalecube.services.auth;

import java.util.List;
import java.util.StringJoiner;
import reactor.core.publisher.Mono;

public record Principal(String role, List<String> permissions) {

  public static final Principal NULL_PRINCIPAL = new Principal(null, null);

  public static Mono<Principal> deferSecured() {
    return deferSecured(Principal.class);
  }

  public static <T> Mono<T> deferSecured(Class<T> clazz) {
    return Mono.deferContextual(context -> Mono.just(context.get(clazz)));
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Principal.class.getSimpleName() + "[", "]")
        .add("role='" + role + "'")
        .add("permissions=" + permissions)
        .toString();
  }
}
