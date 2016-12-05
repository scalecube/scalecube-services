package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class RemoteServiceInstance implements ServiceInstance {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteServiceInstance.class);

  private final ICluster cluster;
  private final Address address;
  private final String memberId;
  private final String serviceName;
  private final Map<String, String> tags;


  /**
   * Remote service instance constructor to initiate instance.
   * 
   * @param cluster to be used for instance context.
   * @param serviceReference service reference of this instance.
   */
  public RemoteServiceInstance(ICluster cluster, ServiceReference serviceReference, Map<String, String> tags) {
    this.serviceName = serviceReference.serviceName();
    this.cluster = cluster;
    this.address = serviceReference.address();
    this.memberId = serviceReference.memberId();
    this.tags = tags;
  }

  @Override
  public String serviceName() {
    return serviceName;
  }

  @Override
  public Object invoke(Message request, ServiceDefinition definition) throws Exception {
    checkArgument(definition != null, "Service definition can't be null");

    // Resolve method
    String methodName = request.header(ServiceHeaders.METHOD);
    checkArgument(methodName != null, "Method name can't be null");
    Method method = definition.method(methodName);
    checkArgument(method != null,
        "Method '%s' is not registered for service: %s", methodName, definition.serviceInterface());

    // Try to call via messaging
    if (method.getReturnType().equals(CompletableFuture.class)) {
      if (extractGenericReturnType(method).equals(Message.class)) {
        return futureInvoke(request, message -> message);
      } else {
        return futureInvoke(request, Message::data);
      }
    } else if (method.getReturnType().equals(Void.TYPE)) {
      return sendRemote(composeRequest(request, request.correlationId()));
    } else {
      throw new UnsupportedOperationException("Unsupported return type for method: " + method);
    }
  }

  private Type extractGenericReturnType(Method method) {
    Type type = method.getGenericReturnType();
    if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getActualTypeArguments()[0];
    } else {
      return Object.class;
    }
  }

  private CompletableFuture<Object> futureInvoke(final Message request, Function<Message, Object> fn) throws Exception {

    ResponseFuture responseFuture = new ResponseFuture(fn);

    Message requestMessage = composeRequest(request, responseFuture.correlationId());

    CompletableFuture<Void> sendFuture = sendRemote(requestMessage);
    // check that send operation completed successfully else report an error
    sendFuture.whenComplete((success, error) -> {
      if (error != null) {
        LOGGER.debug("cid [{}] send remote service request message failed {} , error {}",
            requestMessage.correlationId(),
            requestMessage, error);

        // if send future faild then complete the response future Exceptionally.
        Optional<ResponseFuture> future = ResponseFuture.get(requestMessage.correlationId());
        if (future.isPresent()) {
          future.get().completeExceptionally(error);
        }
      }
    });
    return responseFuture.future();
  }

  private CompletableFuture<Void> sendRemote(Message requestMessage) {
    final CompletableFuture<Void> messageFuture = new CompletableFuture<>();
    LOGGER.debug("cid [{}] send remote service request message {}", requestMessage.correlationId(), requestMessage);
    this.cluster.send(address, requestMessage, messageFuture);
    return messageFuture;
  }

  private Message composeRequest(Message request, final String correlationId) {
    return Message.withData(request.data())
        .header(ServiceHeaders.SERVICE_REQUEST, serviceName)
        .header(ServiceHeaders.METHOD, request.header(ServiceHeaders.METHOD))
        .correlationId(correlationId)
        .build();
  }

  @Override
  public String memberId() {
    return this.memberId;
  }

  public Address address() {
    return address;
  }

  @Override
  public Boolean isLocal() {
    return false;
  }

  public boolean isReachable() {
    return cluster.member(this.memberId).isPresent();
  }

  @Override
  public String toString() {
    return "RemoteServiceInstance [address=" + address
        + ", memberId=" + memberId
        + "]";
  }

  @Override
  public Map<String, String> tags() {
    return tags;
  }
}
