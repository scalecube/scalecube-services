package io.scalecube.gateway.websocket;

import static io.scalecube.gateway.websocket.WebSocketSession.DEFAULT_CONTENT_TYPE;

import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ExceptionProcessor;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public final class WebSocketAcceptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketAcceptor.class);

  private final ServiceCall serviceCall;

  private static final ObjectMapper objectMapper;

  static {
    objectMapper = initMapper();
  }

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
          ServiceMessage request;
          try {
            request = toMessage(frame);
            return serviceCall.requestMany(request);
          } catch (Throwable ex) {
            return Flux.just(ExceptionProcessor.toMessage(ex));
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

  private ByteBuf toByteBuf(ServiceMessage message) {
    ServiceMessage resultMessage = message;

    if (message.hasData(ByteBuf.class)) {
      ByteBuf byteBuf = message.data();
      try (InputStream stream = new ByteBufInputStream(byteBuf.slice())) {
        resultMessage = ServiceMessage.from(message)
            .data(objectMapper.readTree(stream))
            .build();
      } catch (Throwable ex) {
        LOGGER.error("Failed to decode data on: {}, cause: {}, data buffer: {}",
            message, ex, byteBuf.toString(Charset.defaultCharset()));
        throw new BadRequestException("Failed to decode data on message q=" + message.qualifier());
      } finally {
        ReferenceCountUtil.release(byteBuf);
      }
    }

    ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    try (OutputStream stream = new ByteBufOutputStream(byteBuf)) {
      objectMapper.writeValue(stream, resultMessage);
      return byteBuf;
    } catch (Throwable ex) {
      ReferenceCountUtil.release(byteBuf);
      LOGGER.error("Failed to encode message: {}, cause: {}", resultMessage, ex);
      throw new BadRequestException("Failed to encode message q=" + resultMessage.qualifier());
    }
  }

  private ServiceMessage toMessage(WebSocketFrame frame) {
    ByteBuf content = frame.content().slice();
    ServiceMessage.Builder builder = ServiceMessage.builder();

    try (InputStream stream = new ByteBufInputStream(content)) {
      Map decoded = objectMapper.readValue(stream, HashMap.class);
      // noinspection unchecked
      return builder
          .headers((Map) decoded.get("headers"))
          .data(decoded.get("data"))
          .dataFormat(DEFAULT_CONTENT_TYPE)
          .build();
    } catch (Throwable ex) {
      LOGGER.error("Failed to decode message headers: {}, cause: {}",
          content.toString(Charset.defaultCharset()), ex);
      throw new BadRequestException("Failed to decode message headers {headers=" + content.readableBytes()
          + ", data=" + content.readableBytes() + "}");
    } finally {
      ReferenceCountUtil.release(content);
    }
  }

  private static ObjectMapper initMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    mapper.registerModule(new JavaTimeModule());
    return mapper;
  }
}
