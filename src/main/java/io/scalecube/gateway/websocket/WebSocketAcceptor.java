package io.scalecube.gateway.websocket;

import io.scalecube.gateway.core.GatewayMessage;
import io.scalecube.gateway.core.GatewayMessageCodec;
import io.scalecube.gateway.core.Signal;
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
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

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
          Long sid = null;
          try {
            GatewayMessage gatewayMessage = toMessage(frame);
            Long streamId = gatewayMessage.streamId();
            sid = streamId;
            if (gatewayMessage.qualifier() == null) {
              throw new BadRequestException("q is missing");
            }
            if (streamId == null) {
              throw new BadRequestException("sid is missing");
            }
            ServiceMessage request = GatewayMessage.toServiceMessage(gatewayMessage);
            AtomicBoolean isFailure = new AtomicBoolean(false);
            Flux<ServiceMessage> serviceMessages = serviceCall.requestMany(request);
            if (gatewayMessage.inactivity() != null) {
              Duration inactivity = Duration.ofSeconds(gatewayMessage.inactivity());
              serviceMessages = serviceMessages.timeout(inactivity);
            }
            return serviceMessages
                .map(serviceMessage -> {
                  GatewayMessage.Builder builder = GatewayMessage.from(serviceMessage).streamId(streamId);
                  if (ExceptionProcessor.isError(serviceMessage)) {
                    isFailure.set(true);
                    builder.signal(Signal.ERROR);
                  }
                  return builder.build();
                })
                .concatWith(Flux.defer(() -> isFailure.get() ? Flux.empty()
                    : Flux.just(GatewayMessage.builder().streamId(streamId).signal(Signal.COMPLETE).build())))
                .onErrorResume(
                    t -> Flux.just(GatewayMessage.builder().streamId(streamId).signal(Signal.ERROR).build()));
          } catch (Throwable ex) {
            ServiceMessage serviceMessage = ExceptionProcessor.toMessage(ex);
            return Flux.just(GatewayMessage.from(serviceMessage).streamId(sid).signal(Signal.ERROR).build());
          }
        })
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
