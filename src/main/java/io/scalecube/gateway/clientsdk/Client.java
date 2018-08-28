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

  /**
   * Constructor for client.
   *
   * @param transport client transport
   * @param messageCodec client message codec
   */
  public Client(ClientTransport transport, ClientMessageCodec messageCodec) {
    this.transport = transport;
    this.messageCodec = messageCodec;
  }

  /**
   * Close transport function.
   *
   * @return mono void
   */
  public Mono<Void> close() {
    return Mono.defer(transport::close);
  }

  /**
   * Proxy creator function.
   *
   * @param serviceClazz service interface.
   * @param <T> type of service interface.
   * @return proxied service object.
   */
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

  /**
   * Request with mono response as response.
   *
   * @param clientMessage client request message.
   * @return mono response
   */
  public Mono<ClientMessage> requestResponse(ClientMessage clientMessage) {
    return transport.requestResponse(clientMessage);
  }

  /**
   * Request with flux stream as response.
   *
   * @param clientMessage client request message.
   * @return flux response
   */
  public Flux<ClientMessage> requestStream(ClientMessage clientMessage) {
    return transport.requestStream(clientMessage);
  }
}
