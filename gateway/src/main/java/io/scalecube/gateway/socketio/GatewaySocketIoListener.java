package io.scalecube.gateway.socketio;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.ipc.Event;
import io.scalecube.ipc.EventStream;
import io.scalecube.ipc.codec.ServiceMessageCodec;
import io.scalecube.ipc.netty.ChannelSupport;
import io.scalecube.socketio.Session;
import io.scalecube.socketio.SocketIOListener;
import io.scalecube.transport.Address;

import io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * SocketIO listener integrated with {@link EventStream}.
 */
public final class GatewaySocketIoListener implements SocketIOListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewaySocketIoListener.class);

  /**
   * A mapping between socketio {@link Session} identifier and our own generated {@link ChannelContext} identifier. Map
   * is updated when corresponding {@link Session} disconnects.
   */
  private final ConcurrentMap<String, String> sessionIdToChannelContextId = new ConcurrentHashMap<>();

  private final EventStream eventStream;

  public GatewaySocketIoListener(EventStream eventStream) {
    this.eventStream = eventStream;
  }

  @Override
  public void onConnect(Session session) {
    // create channel context
    InetSocketAddress remoteAddress = (InetSocketAddress) session.getRemoteAddress();
    String host = remoteAddress.getAddress().getHostAddress();
    int port = remoteAddress.getPort();
    ChannelContext channelContext = ChannelContext.create(Address.create(host, port));

    // save mapping
    sessionIdToChannelContextId.put(session.getSessionId(), channelContext.getId());

    // register cleanup process upfront
    channelContext.listenClose(input -> {
      if (session.getState() == Session.State.CONNECTED) {
        session.disconnect();
      }
    });

    // bind channelContext
    eventStream.subscribe(channelContext);

    channelContext.listenWrite().map(Event::getMessageOrThrow).subscribe(
        message -> {
          ByteBuf buf = ServiceMessageCodec.encode(message);
          ChannelSupport.releaseRefCount(message.getData()); // release ByteBuf
          try {
            session.send(buf);
            channelContext.postWriteSuccess(message);
          } catch (Exception throwable) {
            channelContext.postWriteError(message, throwable);
          }
        },
        throwable -> {
          LOGGER.error("Fatal exception occured on channel context: {}, cause: {}", channelContext.getId(), throwable);
          session.disconnect();
        });
  }

  @Override
  public void onMessage(Session session, ByteBuf buf) {
    String channelContextId = sessionIdToChannelContextId.get(session.getSessionId());
    if (channelContextId == null) {
      LOGGER.error("Can't find channel context id by session id: {}", session.getSessionId());
      ChannelSupport.releaseRefCount(buf);
      session.disconnect();
      return;
    }

    ChannelContext channelContext = ChannelContext.getIfExist(channelContextId);
    if (channelContext == null) {
      ChannelSupport.releaseRefCount(buf);
      LOGGER.error("Failed to handle message, channel context is null by id: {}", channelContextId);
      session.disconnect();
      return;
    }

    try {
      channelContext.postReadSuccess(ServiceMessageCodec.decode(buf));
    } catch (Exception throwable) {
      ChannelSupport.releaseRefCount(buf);
      channelContext.postReadError(throwable);
    }
  }

  @Override
  public void onDisconnect(Session session) {
    String channelContextId = sessionIdToChannelContextId.remove(session.getSessionId());
    if (channelContextId == null) {
      LOGGER.error("Can't find channel context id by session id: {}", session.getSessionId());
      return;
    }
    ChannelContext.closeIfExist(channelContextId);
  }
}
