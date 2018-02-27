package io.scalecube.gateway.http;

import static io.scalecube.ipc.Qualifier.ERROR_NAMESPACE;

import io.scalecube.ipc.ChannelContext;
import io.scalecube.ipc.ErrorData;
import io.scalecube.ipc.Qualifier;
import io.scalecube.ipc.ServiceMessage;
import io.scalecube.ipc.netty.ChannelSupport;

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

    channelContext.listenMessageWrite().subscribe(
        event -> {
          ServiceMessage message = event.getMessage().get();
          Qualifier qualifier = Qualifier.fromString(message.getQualifier());
          FullHttpResponse response;

          if (!ERROR_NAMESPACE.equalsIgnoreCase(qualifier.getNamespace())) {
            response = message.hasData()
                ? HttpCodecUtil.okResponse((ByteBuf) message.getData())
                : HttpCodecUtil.emptyResponse();
          } else {
            if (message.dataOfType(ErrorData.class)) { // => ErrorData
              response = HttpCodecUtil.errorResponse(qualifier, (ErrorData) message.getData());
            } else if (message.dataOfType(ByteBuf.class)) { // => ByteBuf assumed
              response = HttpCodecUtil.errorResponse(qualifier, (ByteBuf) message.getData());
            } else {
              response = HttpCodecUtil.emptyErrorResponse(qualifier);
            }
          }

          // Hint: at this point we could add logic on writing headers to the response
          ctx.writeAndFlush(response).addListener(
              (ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                  channelContext.postWriteError(future.cause(), message);
                } else {
                  channelContext.postWriteSuccess(message);
                }
              });
        },
        throwable -> {
          LOGGER.error("Fatal exception occured on channel context: {}, cause: {}", channelContext.getId(), throwable);
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

    try {
      FullHttpRequest request = (FullHttpRequest) msg;
      // get qualifier
      // Hint: qualifier format could be changed to start from '/' there be saving from substringing
      String qualifier = request.uri().substring(1);

      ServiceMessage.Builder builder = ServiceMessage.withQualifier(qualifier);
      if (request.content().isReadable()) {
        builder.data(request.content());
      }

      channelContext.postReadSuccess(builder.build());
    } catch (Exception throwable) {
      // Hint: at this point we could save at least request headers and put them into error
      ChannelSupport.releaseRefCount(msg);
      channelContext.postReadError(throwable);
    }
  }
}
