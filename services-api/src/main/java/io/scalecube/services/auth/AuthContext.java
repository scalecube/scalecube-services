package io.scalecube.services.auth;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AuthContext {

  private final Map<String, String> authData;

  /**
   * Constructor.
   *
   * @param authData auth data
   */
  public AuthContext(Map<String, String> authData) {
    Objects.requireNonNull(authData, "authData");
    this.authData = Collections.unmodifiableMap(new HashMap<>(authData));
  }

  public Map<String, String> authData() {
    return authData;
  }
}
