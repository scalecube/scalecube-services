package io.scalecube.services.auth;

/**
 * Represents authentication context.
 *
 * @param <C> credentials type
 * @param <P> principal type
 */
public class AuthContext<C, P> {

  private final C credentials;
  private final P principal;

  public AuthContext(C credentials, P principal) {
    this.credentials = credentials;
    this.principal = principal;
  }

  public C credentials() {
    return credentials;
  }

  public P principal() {
    return principal;
  }
}
