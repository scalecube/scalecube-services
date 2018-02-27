package io.scalecube.gateway.http;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

@Sharable
public final class CorsHeadersHandler extends ChannelInboundHandlerAdapter {

  private final GatewayHttpServer.Config config;

  public CorsHeadersHandler(GatewayHttpServer.Config config) {
    this.config = config;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (!(msg instanceof FullHttpRequest)) {
      super.channelRead(ctx, msg);
      return;
    }

    FullHttpRequest request = (FullHttpRequest) msg;
    if (!(HttpMethod.OPTIONS.equals(request.method()))) {
      super.channelRead(ctx, msg);
      return;
    }

    HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

    response.headers().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, config.getAccessControlAllowOrigin());
    response.headers().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, config.getAccessControlAllowMethods());
    String accessControlRequestHeaders = request.headers().get(HttpHeaderNames.ACCESS_CONTROL_REQUEST_HEADERS);
    if (accessControlRequestHeaders != null) {
      response.headers().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, accessControlRequestHeaders);
    }
    response.headers().add(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE, config.getAccessControlMaxAge());
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);

    ctx.writeAndFlush(response);
  }
}
