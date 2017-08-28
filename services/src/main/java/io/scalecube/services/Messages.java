package io.scalecube.services;

import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.transport.Message;
import io.scalecube.transport.Message.Builder;

public class Messages {

  /**
   * helper method to get service request builder with needed headers.
   * 
   * @param serviceName the requested service name.
   * @param methodName the requested service method name.
   * @return Builder for requested message.
   */
  public static Builder request(String serviceName, String methodName) {
    return Message.builder()
        .header(ServiceHeaders.SERVICE_REQUEST, serviceName)
        .header(ServiceHeaders.METHOD, methodName)
        .correlationId(IdGenerator.generateId());

  }

  /**
   * converts a message to a service request message with correlation id.
   * 
   * @param request with SERVICE_REQUEST and METHOD to copy.
   * @param correlationId for the new request.
   * @return service request message with correlation id.
   */
  public static Message asRequest(Message request, final String correlationId) {
    return Message.withData(request.data())
        .header(ServiceHeaders.SERVICE_REQUEST, request.header(ServiceHeaders.SERVICE_REQUEST))
        .header(ServiceHeaders.METHOD, request.header(ServiceHeaders.METHOD))
        .correlationId(correlationId)
        .build();
  }

  /**
   * utility method to build service response message.
   * 
   * @param data to be use for the response.
   * @param correlationId of a the given request
   * @return response message or response error message in case data is exception.
   */
  public static Message asResponse(Object data, String correlationId) {
    if (data instanceof Message) {
      Message msg = (Message) data;
      return (Message.builder().data(msg.data()).correlationId(correlationId).build());
    } else if (data instanceof Throwable) {
      return (Message.builder().data(data)
          .correlationId(correlationId)
          .header(ServiceHeaders.EXCEPTION, "")
          .build());
    } else {
      return Message.builder()
          .data(data)
          .correlationId(correlationId)
          .build();
    }
  }

  /**
   * build unsubscribed service request for the original correltion id which as subscription was created with.
   * 
   * @param correlationId which the original request that created the subscription.
   * @return unsubscribed request message.
   */
  public static Message asUnsubscribeRequest(final String correlationId) {
    return Message.builder().header(ServiceHeaders.DISPATCHER_SERVICE, ServiceHeaders.UNSUBSCIBE)
        .correlationId(correlationId)
        .build();
  }

}
