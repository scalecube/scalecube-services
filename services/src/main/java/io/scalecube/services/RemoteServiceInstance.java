package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.streams.ClientStreamProcessors;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.codec.StreamMessageDataCodec;
import io.scalecube.streams.codec.StreamMessageDataCodecImpl;
import io.scalecube.transport.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class RemoteServiceInstance implements ServiceInstance {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteServiceInstance.class);

  private final Address address;
  private final String memberId;
  private final String serviceName;
  private final Map<String, String> tags;
  private final ClientStreamProcessors client;
  private final Set<String> methods;
  private StreamMessageDataCodec dataCodec = new StreamMessageDataCodecImpl();

  /**
   * Remote service instance constructor to initiate instance.
   * 
   * @param client to be used for communication.
   * @param serviceReference service reference of this instance.
   * @param tags describing this service instance metadata.
   */
  public RemoteServiceInstance(ClientStreamProcessors client, ServiceReference serviceReference,
      Map<String, String> tags) {

    this.serviceName = serviceReference.serviceName();
    this.methods = Collections.unmodifiableSet(serviceReference.methods());
    this.address = serviceReference.address();
    this.memberId = serviceReference.memberId();
    this.tags = tags;
    this.client = client;
  }

  @Override
  public <T> Observable<T> listen(StreamMessage request, Class<T> responseType) {
    StreamProcessor<StreamMessage, T> sp = client.create(address, responseType);
    sp.onNext(request);
    sp.onCompleted();
    return sp.listen();
  }

  @Override
  public Observable<StreamMessage> listen(StreamMessage request) {
    return listen(request, StreamMessage.class);
  }

  @Override
  public CompletableFuture<StreamMessage> invoke(StreamMessage request) {
    return invoke(request, StreamMessage.class);
  }

  @Override
  public <T> CompletableFuture<StreamMessage> invoke(StreamMessage request, Class<T> responseType) {
    CompletableFuture<StreamMessage> result = new CompletableFuture<>();
    StreamProcessor<StreamMessage, StreamMessage> sp = client.createRaw(address, responseType);
    sp.listen().subscribe(
        onNext -> {
          //LOGGER.info("Rcvd on client: {}", onNext);
          result.complete(onNext);
        },
        onError -> {
          LOGGER.error("Failed to send request {} to target address {}", request, address, onError);
          result.completeExceptionally(onError);
        },
        () -> result.complete(null));

    sp.onNext(request);
    sp.onCompleted();
    return result;
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
