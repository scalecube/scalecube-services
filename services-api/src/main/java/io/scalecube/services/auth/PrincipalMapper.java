package io.scalecube.services.auth;

@FunctionalInterface
public interface PrincipalMapper<O> {

  /**
   * Turns {@code authData} of the {@link AuthContext} to principal.
   *
   * @return converted principle from {@link AuthContext}
   */
  O map(AuthContext authContext);
}
