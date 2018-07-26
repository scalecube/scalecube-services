package io.scalecube.gateway.clientsdk;

import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.services.methods.MethodInfo;

import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

public final class Client {

  private final ClientTransport transport;
  private final ClientMessageCodec messageCodec;

  public Client(ClientTransport transport, ClientMessageCodec messageCodec) {
    this.transport = transport;
    this.messageCodec = messageCodec;
  }

  public Mono<Void> close() {
    return transport.close();
  }

  public <T> T forService(Class<T> serviceClazz) {

    Map<Method, MethodInfo> methods = Reflect.methodsInfo(serviceClazz);
    // noinspection unchecked
    return (T) Proxy.newProxyInstance(
        serviceClazz.getClassLoader(),
        new Class[] {serviceClazz},
        new RemoteInvocationHandler(transport, methods, messageCodec));
  }
}
