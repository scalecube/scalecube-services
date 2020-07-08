package io.scalecube.services.methods;

import java.util.StringJoiner;

public class StubServicePrincipal {

  private final String token;

  public StubServicePrincipal(String token) {
    this.token = token;
  }

  public String token() {
    return token;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", StubServicePrincipal.class.getSimpleName() + "[", "]")
        .add("permissions='" + token + "'")
        .toString();
  }
}
