package io.scalecube.gateway.http;

import static io.scalecube.streams.Qualifier.Q_ERROR_NAMESPACE;

import io.scalecube.streams.ChannelContext;
import io.scalecube.streams.ErrorData;
import io.scalecube.streams.Event;
import io.scalecube.streams.Qualifier;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.netty.ChannelSupport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public final class GatewayHttpMessageHandler extends ChannelDuplexHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayHttpMessageHandler.class);

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    ChannelContext channelContext = ChannelSupport.getChannelContextIfExist(ctx);
    if (channelContext == null) {
      LOGGER.error("Can't find channel context on channel: {}", ctx.channel());
      ctx.channel().close();
      return;
    }

    channelContext.listenWrite().map(Event::getMessageOrThrow).subscribe(
        message -> {
          Qualifier qualifier = Qualifier.fromString(message.qualifier());
          FullHttpResponse response;

          if (!Q_ERROR_NAMESPACE.equalsIgnoreCase(qualifier.getNamespace())) {
            response = message.containsData()
                ? HttpCodecUtil.okResponse((ByteBuf) message.data())
                : HttpCodecUtil.emptyResponse();
          } else {
            if (message.dataOfType(ErrorData.class)) { // => ErrorData
              response = HttpCodecUtil.errorResponse(qualifier, (ErrorData) message.data());
            } else if (message.dataOfType(ByteBuf.class)) { // => ByteBuf assumed
              response = HttpCodecUtil.errorResponse(qualifier, (ByteBuf) message.data());
            } else {
              response = HttpCodecUtil.emptyErrorResponse(qualifier);
            }
          }

          // Hint: at this point we could add logic on writing headers to the response
          ctx.writeAndFlush(response).addListener(
              (ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                  channelContext.postWriteError(message, future.cause());
                } else {
                  channelContext.postWriteSuccess(message);
                }
              });
        },
        throwable -> {
          LOGGER.error("Fatal exception occured on channel context: {}", channelContext.getId(), throwable);
          ctx.channel().close();
        });

    super.channelActive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (!(msg instanceof FullHttpRequest)) {
      super.channelRead(ctx, msg); // pass it through
      return;
    }

    ChannelContext channelContext = ChannelSupport.getChannelContextIfExist(ctx);
    if (channelContext == null) {
      LOGGER.error("Failed to handle message, channel context is null on channel: {}", ctx.channel());
      ChannelSupport.releaseRefCount(msg);
      ctx.channel().close();
      return;
    }

    StreamMessage message = null;
    try {
      FullHttpRequest request = (FullHttpRequest) msg;
      // get qualifier
      // Hint: qualifier format could be changed to start from '/' there be saving from substringing
      String qualifier = request.uri().substring(1);

      StreamMessage.Builder builder = StreamMessage.builder().qualifier(qualifier);
      if (request.content().isReadable()) {
        builder.data(request.content().retain());
      }

      message = builder.build();
    } catch (Exception throwable) {
      // Hint: at this point we could save at least request headers and put them into error
      channelContext.postReadError(throwable);
    } finally {
      ChannelSupport.releaseRefCount(msg);
    }

    if (message != null) {
      channelContext.postReadSuccess(message);
    }
  }
}
