package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteServiceInstance implements ServiceInstance {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteServiceInstance.class);

  private final Address address;
  private final String memberId;
  private final String serviceName;
  private final Map<String, String> tags;
  private final ServiceCommunicator sender;
  private final Set<String> methods;

  private final Subject<Message, Message> serviceResponses;
  private final Observable<Message> transportObservable;

  /**
   * Remote service instance constructor to initiate instance.
   * 
   * @param sender to be used for communication.
   * @param serviceReference service reference of this instance.
   * @param tags describing this service instance metadata.
   */
  public RemoteServiceInstance(ServiceCommunicator sender, ServiceReference serviceReference,
      Map<String, String> tags) {

    this.serviceName = serviceReference.serviceName();
    this.methods = Collections.unmodifiableSet(serviceReference.methods());
    this.address = serviceReference.address();
    this.memberId = serviceReference.memberId();
    this.tags = tags;
    this.sender = sender;

    this.serviceResponses = PublishSubject.<Message>create().toSerialized();
    this.transportObservable = sender.listen();
    if (this.sender.cluster() != null) {
      this.sender.cluster().listenMembership()
          .filter(predicate -> predicate.isRemoved())
          .filter(predicate -> predicate.member().id().equals(this.memberId))
          .subscribe(onNext -> {
            Observer obs = (Observer) serviceResponses;
            obs.onCompleted();
          });
    }
  }

  @Override
  public Observable<Message> listen(final Message request) {

    final String cid = request.correlationId();

    AtomicReference<Subscription> subscription = new AtomicReference<>();
    Observable<Message> observer = transportObservable.doOnUnsubscribe(() -> {
      Message unsubscribeRequest = Messages.asUnsubscribeRequest(cid);
      LOGGER.info("sending remote unsubscribed event: {}", unsubscribeRequest);
      subscription.get().unsubscribe();
      sendRemote(unsubscribeRequest);
    });

    Subscription sub = observer
        .filter(message -> message.correlationId().equals(cid))
        .subscribe(onNext -> {
          serviceResponses.onNext(onNext);
        });

    subscription.set(sub);
    sendRemote(request);
    return serviceResponses;
  }

  /**
   * Dispatch a request message and invoke a service by a given service name and method name. expected headers in
   * request: ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to
   * invoke.
   * 
   * @param request request with given headers.
   * @return CompletableFuture with dispatching transportObservable
   * @throws Exception in case of an error
   */
  public CompletableFuture<Message> dispatch(Message request) throws Exception {
    return invoke(request);
  }


  @Override
  public CompletableFuture<Message> invoke(Message request) {
    Messages.validate().serviceRequest(request);
    CompletableFuture<Message> result = new CompletableFuture<Message>();
    
    try {
      sendRemote(request);
      result.complete(Message.builder().data("remote send completed").build());
    } catch (Exception error) {
      LOGGER.error("Failed to send request {} to target address {}", request, address);
      result.completeExceptionally(error);
    }
    return result;
  }

  private void sendRemote(Message request) {
    LOGGER.debug("cid [{}] send remote service request message {}", request.correlationId(), request);
    this.sender.send(address, request);
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
  public Map<String, String> tags() {
    return tags;
  }

  @Override
  public String serviceName() {
    return serviceName;
  }

  @Override
  public boolean methodExists(String methodName) {
    return methods.contains(methodName);
  }

  @Override
  public void checkMethodExists(String methodName) {
    checkArgument(this.methodExists(methodName), "instance has no such requested method");
  }

  @Override
  public Collection<String> methods() {
    return methods;
  }

  @Override
  public String toString() {
    return "RemoteServiceInstance [serviceName=" + serviceName + ", address=" + address + ", memberId=" + memberId
        + ", methods=" + methods + ", tags=" + tags + "]";
  }


}
