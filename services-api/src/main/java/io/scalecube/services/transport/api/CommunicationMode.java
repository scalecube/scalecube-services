package io.scalecube.services.transport.api;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


public enum CommunicationMode {

  ONE_WAY, // FIRE_AND_FORGET
  REQUEST_ONE, // REQUEST_RESPONSE
  REQUEST_MANY, // REQUEST_STREAM
  REQUEST_STREAM, // REQUEST_CHANNEL
  INVALID;


  static Map<Class<?>, CommunicationMode> map = new HashMap<>(4);
  static {
    map.put(Void.TYPE, ONE_WAY);
    map.put(Flux.class, REQUEST_MANY);
    map.put(Publisher.class, REQUEST_MANY);
    map.put(Mono.class, REQUEST_ONE);
  }

  public static CommunicationMode of(Method m) {
    Class<?> returnType = m.getReturnType();
    Class<?> paramType = null;
    if (m.getParameterTypes().length > 0) {
      paramType = m.getParameterTypes()[0];
    }

    if (paramType != null && (Publisher.class.isAssignableFrom(paramType))) {
      return REQUEST_STREAM;
    } else {
      return map.getOrDefault(returnType, INVALID);
    }
  }

}
