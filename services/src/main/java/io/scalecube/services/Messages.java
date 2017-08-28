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

  public static Message asRequest(Message request, final String correlationId) {
    return Message.withData(request.data())
        .header(ServiceHeaders.SERVICE_REQUEST, request.header(ServiceHeaders.SERVICE_REQUEST))
        .header(ServiceHeaders.METHOD, request.header(ServiceHeaders.METHOD))
        .correlationId(correlationId)
        .build();
  }
  
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
  
  public static Message asUnsubscribeRequest(final Message request) {
    return Message.builder().header(ServiceHeaders.DISPATCHER_SERVICE, ServiceHeaders.UNSUBSCIBE)
        .correlationId(request.correlationId())
        .build();
  }
  
}
