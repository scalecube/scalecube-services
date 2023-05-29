package io.scalecube.services.examples.auth;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.ForbiddenException;
import reactor.core.publisher.Mono;

public class SecuredServiceByUserProfileImpl implements SecuredServiceByUserProfile {

  @Override
  public Mono<String> hello(String name) {
    return Authenticator.deferSecured(UserProfile.class)
        .flatMap(
            user -> {
              checkPermissions(user);
              return Mono.just("Hello, name=" + name + " (user=" + user + ")");
            });
  }

  private void checkPermissions(UserProfile user) {
    if (!user.role().equals("ADMIN")) {
      throw new ForbiddenException("Forbidden");
    }
  }
}
