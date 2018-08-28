package io.scalecube.gateway.clientsdk;

import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.services.methods.MethodInfo;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class Client {

  private final ClientTransport transport;
  private final ClientMessageCodec messageCodec;

  private final ConcurrentHashMap<Class<?>, ? super Object> proxyMap = new ConcurrentHashMap<>();

  public Client(ClientTransport transport, ClientMessageCodec messageCodec) {
    this.transport = transport;
    this.messageCodec = messageCodec;
  }

  public Mono<Void> close() {
    return transport.close();
  }

  public <T> T forService(Class<T> serviceClazz) {
    // noinspection unchecked
    return (T)
        proxyMap.computeIfAbsent(
            serviceClazz,
            (clazz) -> {
              Map<Method, MethodInfo> methods = Reflect.methodsInfo(serviceClazz);
              return Proxy.newProxyInstance(
                  serviceClazz.getClassLoader(),
                  new Class[] {serviceClazz},
                  new RemoteInvocationHandler(transport, methods, messageCodec));
            });
  }

  public Flux<ClientMessage> rawStream(ClientMessage clientMessage) {
    return transport.requestStream(clientMessage);
  }

  public Mono<ClientMessage> rawRequestResponse(ClientMessage clientMessage) {
    return transport.requestResponse(clientMessage);
  }
}
