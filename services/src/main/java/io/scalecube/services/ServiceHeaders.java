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
   * This header is supposed to be used by application in case when sending service request. the service-request header
   * is used to mark a request as a service request.
   */
  public static final String SERVICE_REQUEST = "service-request";

  /**
   * This header is supposed to be used by application in case when sending service response. the service-response
   * header is used to mark a request as a service response.
   */
  public static final String SERVICE_RESPONSE = "service-response";

  /**
   * This header is supposed to be used by application in case when sending service error response.
   */
  public static final String EXCEPTION = "exception";

  public static final String UNSUBSCIBE = "unsubscribe";

  public static final String OBSERVER = "observer";

  private ServiceHeaders() {
    // Do not instantiate
  }

  /**
   * Extract header value from a given message. the service method to invoke on request.
   * 
   * @param request request message
   * @return header value
   */
  public static String serviceMethod(Message request) {
    return request.header(METHOD);
  }

  /**
   * Extract header value from a given message. is used to mark a request as a service request.
   * 
   * @param request request message
   * @return header value
   */
  public static String serviceRequest(Message request) {
    return request.header(SERVICE_REQUEST);
  }

  /**
   * Extract header value from a given message. header is used to mark a request as a service response.
   *
   * @param request request message
   * @return header value
   */
  public static String serviceResponse(Message request) {
    return request.header(SERVICE_RESPONSE);
  }
}
