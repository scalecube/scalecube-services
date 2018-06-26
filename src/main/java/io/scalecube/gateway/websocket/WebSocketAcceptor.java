package io.scalecube.gateway.websocket;

import io.scalecube.gateway.core.GatewayMessage;
import io.scalecube.gateway.core.GatewayMessageCodec;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ExceptionProcessor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public final class WebSocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketAcceptor.class);

  private final ServiceCall serviceCall;

  private final GatewayMessageCodec gatewayMessageCodec = new GatewayMessageCodec();

  public WebSocketAcceptor(ServiceCall serviceCall) {
    this.serviceCall = serviceCall;
  }

  /**
   * Connect handler method.
   *
   * @param session websocket session.
   * @return mono void.
   */
  public Mono<Void> onConnect(WebSocketSession session) {
    LOGGER.info("Session connected: " + session);

    Flux<ByteBuf> messages = session.receive()
        .flatMap(frame -> {
          try {
            ServiceMessage request = GatewayMessage.toServiceMessage(toMessage(frame));
            return serviceCall.requestMany(request);
          } catch (Throwable ex) {
            return Flux.just(ExceptionProcessor.toMessage(ex));
          }
        })
        .map(GatewayMessage::toGatewayMessage)
        .map(this::toByteBuf)
        .doOnError(throwable -> session.close());

    Mono<Void> voidMono = session.send(messages);

    session.onClose(() -> LOGGER.info("Session disconnected: bye bye"));

    return voidMono.then();
  }

  /**
   * Diconnect handler method.
   *
   * @param session websocket session.
   * @return mono void.
   */
  public Mono<Void> onDisconnect(WebSocketSession session) {
    LOGGER.info("Session disconnected: " + session);
    return Mono.empty();
  }

  private ByteBuf toByteBuf(GatewayMessage message) {
    try {
      return gatewayMessageCodec.encode(message);
    } catch (Throwable ex) {
      LOGGER.error("Failed to encode message: {}, cause: {}", message, ex);
      throw new BadRequestException("Failed to encode message q=" + message.qualifier());
    }
  }

  private GatewayMessage toMessage(WebSocketFrame frame) {
    ByteBuf content = frame.content().slice();
    try {
      return gatewayMessageCodec.decode(content);
    } catch (Throwable ex) {
      LOGGER.error("Failed to decode message headers: {}, cause: {}",
          content.toString(Charset.defaultCharset()), ex);
      throw new BadRequestException("Failed to decode message headers {headers=" + content.readableBytes()
          + ", data=" + content.readableBytes() + "}");
    } finally {
      // ReferenceCountUtil.release(content);
    }
  }
}
