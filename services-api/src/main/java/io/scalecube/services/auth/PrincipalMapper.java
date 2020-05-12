package io.scalecube.services.auth;

import java.util.Map;

@FunctionalInterface
public interface PrincipalMapper<O> {

  /**
   * Turns {@code authData} to concrete principal object.
   *
   * @param authData auth data
   * @return converted principle object
   */
  O map(Map<String, String> authData);
}
