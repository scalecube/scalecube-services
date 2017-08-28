package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class RemoteServiceInstance implements ServiceInstance {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteServiceInstance.class);

  private final Address address;
  private final String memberId;
  private final String serviceName;
  private final Map<String, String> tags;
  private final ServiceCommunicator sender;


  /**
   * Remote service instance constructor to initiate instance.
   * 
   * @param sender to be used for communication.
   * @param serviceReference service reference of this instance.
   * @param tags describing this service instance metadata.
   */
  public RemoteServiceInstance(ServiceCommunicator sender,
      ServiceReference serviceReference,
      Map<String, String> tags) {
    this.serviceName = serviceReference.serviceName();
    this.address = serviceReference.address();
    this.memberId = serviceReference.memberId();
    this.tags = tags;
    this.sender = sender;
  }

  @Override
  public Observable<Message> listen(final Message request) {

    final Observable<Message> result = sender.listen()
        .doOnUnsubscribe(() -> {
          sendRemote(Messages.asUnsubscribeRequest(request));
        }).filter(
            message -> message.correlationId().equals(request.correlationId()));

    sendRemote(request);

    return result;
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
  public CompletableFuture<Message> dispatch(Message request) throws Exception {
    return invoke(request);
  }


  @Override
  public CompletableFuture<Message> invoke(Message request) {
    checkArgument(request != null, "Service request can't be null");
    CompletableFuture<Message> result = new CompletableFuture<Message>();

    futureInvoke(request)
        .whenComplete((success, error) -> {
          if (error == null) {
            result.complete(Message.builder().data("remote send completed").build());
          } else {
            result.completeExceptionally(error);
          }
        });

    return result;
  }

  private CompletableFuture<Void> futureInvoke(final Message request) {

    final String methodName = request.header(ServiceHeaders.METHOD);
    checkArgument(methodName != null, "Method name can't be null");

    final String serviceName = request.header(ServiceHeaders.SERVICE_REQUEST);
    checkArgument(serviceName != null, "Service request can't be null");

    return sendRemote(request);
  }

  private CompletableFuture<Void> sendRemote(Message requestMessage) {
    LOGGER.debug("cid [{}] send remote service request message {}", requestMessage.correlationId(), requestMessage);
    return this.sender.send(address, requestMessage);
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

  @Override
  public String serviceName() {
    return serviceName;
  }
}
