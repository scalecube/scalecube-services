package io.scalecube.services;

/**
 * Static constants for service headers.
 */
public final class ServiceHeaders {

  /**
   * This header is supposed to be used by application in case if same data type can be reused for several messages
   * so it will allow to qualify the specific message type.
   */
  public static final String METHOD = "m";
  public static final String SERVICE = "service";

  private ServiceHeaders() {
    // Do not instantiate
  }

}
