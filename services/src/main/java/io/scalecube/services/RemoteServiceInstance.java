package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.Cluster;
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

  private final Cluster cluster;
  private final Address address;
  private final String memberId;
  private final String serviceName;
  private final Map<String, String> tags;

  private ServiceRegistry serviceRegistry;


  /**
   * Remote service instance constructor to initiate instance.
   * 
   * @param serviceRegistry to be used for instance context.
   * @param serviceReference service reference of this instance.
   * @param tags describing this service instance metadata.
   */
  public RemoteServiceInstance(ServiceRegistry serviceRegistry, ServiceReference serviceReference,
      Map<String, String> tags) {
    this.serviceRegistry = serviceRegistry;
    this.serviceName = serviceReference.serviceName();
    this.cluster = serviceRegistry.cluster();
    this.address = serviceReference.address();
    this.memberId = serviceReference.memberId();
    this.tags = tags;
  }

  @Override
  public String serviceName() {
    return serviceName;
  }

  /**
   * Dispatch a request message and invoke a service by a given service name and method name. expected headers in
   * request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
   * invoke.
   * 
   * @param request request with given headers.
   * @return CompletableFuture with dispatching result
   * @throws Exception in case of an error
   */
  public CompletableFuture<Object> dispatch(Message request) throws Exception {
    ServiceResponse responseFuture = new ServiceResponse(fn -> request);
    Message requestMessage = composeRequest(request, responseFuture.correlationId());

    // Resolve method
    String methodName = request.header(ServiceHeaders.METHOD);
    checkArgument(methodName != null, "Method name can't be null");

    String serviceName = request.header(ServiceHeaders.SERVICE_REQUEST);
    checkArgument(serviceName != null, "Service request can't be null");

    return futureInvoke(requestMessage, message -> message);
  }

  @Override
  public Object invoke(Message request) throws Exception {
    checkArgument(request != null, "Service request can't be null");

    // Resolve method
    String methodName = request.header(ServiceHeaders.METHOD);
    checkArgument(methodName != null, "Method name can't be null");
    ServiceDefinition definition = serviceRegistry.getServiceDefinition(serviceName).get();
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
      return futureInvoke(request, message -> request.correlationId());
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

    ServiceResponse responseFuture = new ServiceResponse(fn);

    Message requestMessage = composeRequest(request, responseFuture.correlationId());

    CompletableFuture<Void> sendFuture = sendRemote(requestMessage);
    // check that send operation completed successfully else report an error
    sendFuture.whenComplete((success, error) -> {
      if (error != null) {
        LOGGER.debug("cid [{}] send remote service request message failed {} , error {}",
            requestMessage.correlationId(),
            requestMessage, error);

        // if send future faild then complete the response future Exceptionally.
        Optional<ServiceResponse> future = ServiceResponse.get(requestMessage.correlationId());
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
