package io.scalecube.services.auth;

@FunctionalInterface
public interface PrincipalMapper<A, P> {

  /**
   * Turns {@code authData} to concrete principal object.
   *
   * @param authData auth data
   * @return converted principle object
   */
  P map(A authData);
}
