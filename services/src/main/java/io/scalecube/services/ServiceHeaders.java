package io.scalecube.services;

import io.scalecube.transport.Message;

/**
 * Static constants for service headers.
 */
public final class ServiceHeaders {

  /**
   * This header is supposed to be used by application in case when sending service request. the method header defines
   * the service method to invoke on request.
   */
  public static final String METHOD = "m";

  /**
   * Extract header value from a given message.
   * the service method to invoke on request.
   */
  public static String service_method_of(Message request) {
    return request.header(METHOD);
  }

  /**
   * This header is supposed to be used by application in case when registering a service at discovery. the service
   * header is used to mark this registration as a microservice instance.
   */
  public static final String SERVICE = "service";

  /**
   * Extract header value from a given message.
   * header is used to mark this registration as a microservice instance.
   */
  public static String service_of(Message request) {
    return request.header(SERVICE);
  }

  /**
   * This header is supposed to be used by application in case when sending service request. the service-request header
   * is used to mark a request as a service request.
   */
  public static final String SERVICE_REQUEST = "service-request";

  /**
   * Extract header value from a given message.
   * is used to mark a request as a service request.
   */
  public static String service_request_of(Message request) {
    return request.header(SERVICE_REQUEST);
  }

  /**
   * This header is supposed to be used by application in case when sending service response. the service-response
   * header is used to mark a request as a service response.
   */
  public static final String SERVICE_RESPONSE = "service-response";

  /**
   * Extract header value from a given message.
   * header is used to mark a request as a service response.
   */
  public static String service_response_of(Message request) {
    return request.header(SERVICE_RESPONSE);
  }

  private ServiceHeaders() {
    // Do not instantiate
  }
}
