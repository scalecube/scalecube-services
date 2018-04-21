package io.scalecube.rsockets;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;
import java.util.Optional;


public enum CommunicationMode {

  ONE_WAY, // FIRE_AND_FORGET
  REQUEST_ONE, // REQUEST_RESPONSE
  REQUEST_MANY, // REQUEST_STREAM
  REQUEST_STREAM; // REQUEST_CHANNEL

  static Optional<CommunicationMode> of(Method m) {
    Class<?> returnType = m.getReturnType();
    Class<?> reqType = null;
    
    if (m.getParameterTypes().length > 0) {
      reqType = m.getParameterTypes()[0];
    }
    
    if (reqType != null && (Flux.class == reqType || Publisher.class == reqType)) {
      return Optional.of(REQUEST_STREAM);
    } else if (Void.TYPE == returnType || Void.class == returnType.getGenericSuperclass()) {
      return Optional.of(ONE_WAY);
    } else if (Flux.class.equals(returnType)) {
      return Optional.of(REQUEST_MANY);
    } else if (Mono.class == returnType) {
      return Optional.of(REQUEST_ONE);
    } else if (Publisher.class == returnType) {
      return Optional.of(REQUEST_ONE);
    } else {
      throw new UnsupportedOperationException(
          "Service method must be one of the following patterns:FIRE_N_FORGET, MONO, REQ_STREAM, BIDIRECTIONAL");
    }
  }

}
