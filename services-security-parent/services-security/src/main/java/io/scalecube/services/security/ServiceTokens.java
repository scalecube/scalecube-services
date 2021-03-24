package io.scalecube.services.security;

public final class ServiceTokens {

  public static final String SERVICE_TOKEN_HEADER = "serviceToken";
  public static final String PERMISSIONS_CLAIM = "permissions";
  public static final String LOCAL_TOKEN_HEADER = "localToken";

  private ServiceTokens() {
    // Do not instantiate
  }
}
