package io.scalecube.services;

import static java.util.Objects.requireNonNull;

import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;

public class Messages {

  private static final MessageValidator validator = new MessageValidator();

  /**
   * message validation utility class for validation and checking arguments of message.
   */
  public static final class MessageValidator {

    /**
     * validates that a request has ServiceHeaders.SERVICE_REQUEST header and ServiceHeaders.METHOD.
     * 
     * @param request message that is subject to validation.
     */
    public void serviceRequest(ServiceMessage request) {
      requireNonNull(request != null, "Service request can't be null");
      final String serviceName = request.qualifier();
      requireNonNull(serviceName != null, "Service request can't be null");
    }

  }

  public static final class MessagesBuilder {

    /**
     * helper method to get service request builder with needed headers.
     * 
     * @param serviceName the requested service name.
     * @param methodName the requested service method name.
     * @return Builder for requested message.
     */
    public ServiceMessage.Builder request(String serviceName, String methodName) {
      return ServiceMessage.builder().qualifier(serviceName, methodName);

    }

    /**
     * helper method to get service request builder with needed headers.
     * 
     * @param api service interface class.
     * @param methodName the requested service method name.
     * @return Builder for requested message.
     */
    public ServiceMessage.Builder request(Class<?> api, String methodName) {
      String serviceName = Reflect.serviceName(api);
      return request(serviceName, methodName);
    }

  }

  /**
   * Utility method to build service error response message.
   * 
   * @param error to be use for the response.
   * @param correlationId of a the given request.
   * @param memberId that created the response.
   * @return response message or response error message in case data is exception.
   */
  public static ServiceMessage asError(Throwable error) {
    return ServiceMessage.builder().data(error).build();
  }

  public static MessagesBuilder builder() {
    return new MessagesBuilder();
  }

  public static MessageValidator validate() {
    return validator;
  }

  public static Qualifier qualifierOf(ServiceMessage request) {
    return Qualifier.fromString(request.qualifier());
  }
}
