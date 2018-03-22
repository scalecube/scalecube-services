package io.scalecube.service.streams;

import io.scalecube.transport.Message;

import rx.Observable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class ServiceConsumer {
  
  private final Builder builder;
  
  public ServiceConsumer(Builder builder) {
    this.builder = builder;
  }
  
  public static class Builder {
    private Duration duration = Duration.ofSeconds(3);

    public Builder timeout(Duration duration) {
      this.duration = duration;
      return this;
    }
    
    public ServiceConsumer build(){
      return new ServiceConsumer(this); 
    }
  }

  public Builder builder() {
    return new Builder();
  }
  
  

  /**
   * Invoke a request message and invoke a service by a given service name and method name. expected headers in request:
   * ServiceHeaders.SERVICE_REQUEST the logical name of the service. ServiceHeaders.METHOD the method name to invoke
   * message uses the router to select the target endpoint service instance in the cluster. Throws Exception in case of
   * an error or TimeoutException if no response if a given duration.
   * 
   * @param request request with given headers.
   * @return CompletableFuture with service call dispatching result.
   */
  public CompletableFuture<Message> invoke(Message request) {
    
    return null;
  }

  /**
   * sending subscription request message to a service that returns Observable.
   * 
   * @param request containing subscription data.
   * @return rx.Observable for the specific stream.
   */
  public Observable<Message> listen(Message request) {
    
    return null;
  }


}
