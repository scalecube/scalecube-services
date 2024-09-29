package io.scalecube.services.gateway.client.ws;

import static reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.ReferenceCountUtil;
import io.scalecube.services.gateway.client.GatewayClientCodec;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Map;
import java.util.StringJoiner;
import org.jctools.maps.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.publisher.Sinks.One;
import reactor.netty.Connection;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

public final class WebsocketGatewayClientSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketGatewayClientSession.class);

  private static final ClosedChannelException CLOSED_CHANNEL_EXCEPTION =
      new ClosedChannelException();

  private static final String STREAM_ID = "sid";
  private static final String SIGNAL = "sig";

  private final String id; // keep id for tracing
  private final GatewayClientCodec clientCodec;
  private final Connection connection;

  // processor by sid mapping
  private final Map<Long, Object> inboundProcessors = new NonBlockingHashMapLong<>(1024);

  WebsocketGatewayClientSession(GatewayClientCodec clientCodec, Connection connection) {
    this.id = Integer.toHexString(System.identityHashCode(this));
    this.clientCodec = clientCodec;
    this.connection = connection;

    WebsocketInbound inbound = (WebsocketInbound) connection.inbound();
    inbound
        .receive()
        .retain()
        .subscribe(
            byteBuf -> {
              if (!byteBuf.isReadable()) {
                ReferenceCountUtil.safestRelease(byteBuf);
                return;
              }

              // decode message
              ServiceMessage message;
              try {
                message = clientCodec.decode(byteBuf);
              } catch (Exception ex) {
                LOGGER.error("Response decoder failed:", ex);
                return;
              }

              // ignore messages w/o sid
              if (!message.headers().containsKey(STREAM_ID)) {
                LOGGER.error("Ignore response: {} with null sid, session={}", message, id);
                if (message.data() != null) {
                  ReferenceCountUtil.safestRelease(message.data());
                }
                return;
              }

              // processor?
              long sid = Long.parseLong(message.header(STREAM_ID));
              Object processor = inboundProcessors.get(sid);
              if (processor == null) {
                if (message.data() != null) {
                  ReferenceCountUtil.safestRelease(message.data());
                }
                return;
              }

              // handle response message
              handleResponse(message, processor);
            });

    connection.onDispose(
        () -> inboundProcessors.forEach((k, o) -> emitError(o, CLOSED_CHANNEL_EXCEPTION)));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  <T> One<T> newMonoProcessor(long sid) {
    return (One) inboundProcessors.computeIfAbsent(sid, this::newMonoProcessor0);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  <T> Many<T> newUnicastProcessor(long sid) {
    return (Many) inboundProcessors.computeIfAbsent(sid, this::newUnicastProcessor0);
  }

  private One<Object> newMonoProcessor0(long sid) {
    LOGGER.debug("Put sid={}, session={}", sid, id);
    return Sinks.one();
  }

  private Many<Object> newUnicastProcessor0(long sid) {
    LOGGER.debug("Put sid={}, session={}", sid, id);
    return Sinks.many().unicast().onBackpressureBuffer();
  }

  void removeProcessor(long sid) {
    if (inboundProcessors.remove(sid) != null) {
      LOGGER.debug("Removed sid={}, session={}", sid, id);
    }
  }

  Mono<Void> send(ByteBuf byteBuf) {
    return connection.outbound().sendObject(new TextWebSocketFrame(byteBuf)).then();
  }

  void cancel(long sid, String qualifier) {
    ByteBuf byteBuf =
        clientCodec.encode(
            ServiceMessage.builder()
                .qualifier(qualifier)
                .header(STREAM_ID, sid)
                .header(SIGNAL, Signal.CANCEL.codeAsString())
                .build());

    send(byteBuf)
        .subscribe(
            null,
            th ->
                LOGGER.error("Exception occurred on sending CANCEL signal for session={}", id, th));
  }

  /**
   * Close the websocket session with <i>normal</i> status. <a
   * href="https://tools.ietf.org/html/rfc6455#section-7.4.1">Defined Status Codes:</a> <i>1000
   * indicates a normal closure, meaning that the purpose for which the connection was established
   * has been fulfilled.</i>
   *
   * @return mono void
   */
  public Mono<Void> close() {
    return ((WebsocketOutbound) connection.outbound()).sendClose().then();
  }

  public Mono<Void> onClose() {
    return connection.onDispose();
  }

  private void handleResponse(ServiceMessage response, Object processor) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Handle response: {}, session={}", response, id);
    }

    try {
      Signal signal = null;
      final String header = response.header(SIGNAL);

      if (header != null) {
        signal = Signal.from(header);
      }

      if (signal == null) {
        // handle normal response
        emitNext(processor, response);
      } else {
        // handle completion signal
        if (signal == Signal.COMPLETE) {
          emitComplete(processor);
        }
        if (signal == Signal.ERROR) {
          // decode error data to retrieve real error cause
          emitNext(processor, clientCodec.decodeData(response, ErrorData.class));
        }
      }
    } catch (Exception e) {
      emitError(processor, e);
    }
  }

  private static void emitNext(Object processor, ServiceMessage message) {
    if (processor instanceof One) {
      //noinspection unchecked
      ((One<ServiceMessage>) processor).emitValue(message, busyLooping(Duration.ofSeconds(3)));
    }
    if (processor instanceof Many) {
      //noinspection unchecked
      ((Many<ServiceMessage>) processor).emitNext(message, busyLooping(Duration.ofSeconds(3)));
    }
  }

  private static void emitComplete(Object processor) {
    if (processor instanceof One) {
      ((One<?>) processor).emitEmpty(busyLooping(Duration.ofSeconds(3)));
    }
    if (processor instanceof Many) {
      ((Many<?>) processor).emitComplete(busyLooping(Duration.ofSeconds(3)));
    }
  }

  private static void emitError(Object processor, Exception e) {
    if (processor instanceof One) {
      ((One<?>) processor).emitError(e, busyLooping(Duration.ofSeconds(3)));
    }
    if (processor instanceof Many) {
      ((Many<?>) processor).emitError(e, busyLooping(Duration.ofSeconds(3)));
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", WebsocketGatewayClientSession.class.getSimpleName() + "[", "]")
        .add("id=" + id)
        .toString();
  }
}
