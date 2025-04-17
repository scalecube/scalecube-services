package io.scalecube.services.sut.security;

import io.scalecube.services.RequestContext;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.auth.Secured;
import io.scalecube.services.exceptions.ForbiddenException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;
import reactor.core.publisher.Mono;

@Secured
public class CompositeSecuredServiceImpl implements CompositeSecuredService {

  // Services secured by code in method body

  @Secured
  @Override
  public Mono<Void> helloComposite() {
    return RequestContext.deferContextual()
        .doOnNext(
            context -> {
              if (!context.hasPrincipal()) {
                throw new ForbiddenException("Insufficient permissions");
              }

              final var principal = context.principal();
              final var permissions = principal.permissions();

              if (!permissions.contains("helloComposite")) {
                throw new ForbiddenException("Insufficient permissions");
              }
            })
        .then();
  }

  public static class CompositePrincipalImpl implements Principal {

    private final Principal principal;
    private final List<String> permissions = new ArrayList<>();

    public CompositePrincipalImpl(Principal principal, List<String> permissions) {
      this.principal = principal;
      if (principal.permissions() != null) {
        this.permissions.addAll(principal.permissions());
      }
      if (permissions != null) {
        this.permissions.addAll(permissions);
      }
    }

    @Override
    public String role() {
      return principal.role();
    }

    @Override
    public boolean hasRole(String role) {
      return principal.hasRole(role);
    }

    @Override
    public Collection<String> permissions() {
      return permissions;
    }

    @Override
    public boolean hasPermission(String permission) {
      return permissions.contains(permission);
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", CompositePrincipalImpl.class.getSimpleName() + "[", "]")
          .add("principal=" + principal)
          .add("permissions=" + permissions)
          .toString();
    }
  }
}
