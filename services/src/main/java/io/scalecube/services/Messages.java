package io.scalecube.services;

import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.transport.Message;
import io.scalecube.transport.Message.Builder;

public class Messages {

  public static final class MessagesBuilder {

    /**
     * helper method to get service request builder with needed headers.
     * 
     * @param serviceName the requested service name.
     * @param methodName the requested service method name.
     * @return Builder for requested message.
     */
    public Builder request(String serviceName, String methodName) {
      return Message.builder()
          .header(ServiceHeaders.SERVICE_REQUEST, serviceName)
          .header(ServiceHeaders.METHOD, methodName)
          .correlationId(IdGenerator.generateId());
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
      return Message.builder()
          .header(ServiceHeaders.SERVICE_REQUEST, serviceName)
          .header(ServiceHeaders.METHOD, methodName)
          .correlationId(IdGenerator.generateId());
    }

  }

  /**
   * converts a message to a service request message with correlation id.
   * 
   * @param request with SERVICE_REQUEST and METHOD to copy.
   * @param correlationId for the new request.
   * @return service request message with correlation id.
   */
  public static Message asRequest(Message request, final String correlationId) {
    return Message.builder()
        .headers(request.headers())
        .data(request.data())
        .correlationId(correlationId)
        .build();
  }

  /**
   * utility method to build service response message.
   * 
   * @param data to be use for the response.
   * @param correlationId of a the given request.
   * @param memberId that created the response.
   * @return response message or response error message in case data is exception.
   */
  public static Message asResponse(Object data, String correlationId, String memberId) {

    Builder builder = Message.builder()
        .correlationId(correlationId)
        .header("memberId", memberId);

    if (data instanceof Message) {
      Message msg = (Message) data;
      builder = builder.data(msg.data());
    } else {
      builder = builder.data(data);
      if (data instanceof Throwable) {
        builder = builder.header(ServiceHeaders.EXCEPTION, "");
      }
    }

    return builder.build();
  }

  /**
   * Utility method to build service error response message.
   * 
   * @param error to be use for the response.
   * @param correlationId of a the given request.
   * @param memberId that created the response.
   * @return response message or response error message in case data is exception.
   */
  public static Message asError(Throwable error, String correlationId, String memberId) {
    return asResponse(error, correlationId, memberId);
  }

  public static MessagesBuilder builder() {
    return new MessagesBuilder();
  }


}
