package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.streams.Qualifier;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamMessage.Builder;

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
    public void serviceRequest(StreamMessage request) {
      checkArgument(request != null, "Service request can't be null");
      final String serviceName = request.qualifier();
      checkArgument(serviceName != null, "Service request can't be null");
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
    public StreamMessage.Builder request(String serviceName, String methodName) {
      return StreamMessage.builder().qualifier(serviceName, methodName);

    }

    /**
     * helper method to get service request builder with needed headers.
     * 
     * @param api service interface class.
     * @param methodName the requested service method name.
     * @return Builder for requested message.
     */
    public Builder request(Class<?> api, String methodName) {
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
  public static StreamMessage asError(Throwable error) {
    return StreamMessage.builder().data(error).build();
  }

  public static MessagesBuilder builder() {
    return new MessagesBuilder();
  }

  public static MessageValidator validate() {
    return validator;
  }

  public static Qualifier qualifierOf(StreamMessage request) {
    return Qualifier.fromString(request.qualifier());
  }
}
