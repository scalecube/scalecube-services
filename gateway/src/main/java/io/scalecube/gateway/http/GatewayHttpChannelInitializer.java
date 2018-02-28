package io.scalecube.gateway.http;

import io.scalecube.ipc.EventStream;
import io.scalecube.ipc.netty.ChannelContextHandler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;

@Sharable
public final class GatewayHttpChannelInitializer extends ChannelInitializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewayHttpChannelInitializer.class);

  private final GatewayHttpServer.Config config;
  private final ChannelContextHandler channelContextHandler;
  private final CorsHeadersHandler corsHeadersHandler;
  private final GatewayHttpMessageHandler messageHandler;

  /**
   * Constructs new initializer with config.
   */
  public GatewayHttpChannelInitializer(GatewayHttpServer.Config config) {
    this.config = config;
    this.corsHeadersHandler = new CorsHeadersHandler(config);
    this.messageHandler = new GatewayHttpMessageHandler();
    EventStream serverStream = config.getServerStream();
    this.channelContextHandler = new ChannelContextHandler(serverStream::subscribe);
  }

  @Override
  protected void initChannel(Channel channel) {
    ChannelPipeline pipeline = channel.pipeline();
    // contexs contexts contexs
    channel.pipeline().addLast(channelContextHandler);
    // set ssl if present
    if (config.getSslContext() != null) {
      SSLEngine sslEngine = config.getSslContext().createSSLEngine();
      sslEngine.setUseClientMode(false);
      pipeline.addLast(new SslHandler(sslEngine));
    }
    // add http codecs
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpObjectAggregator(config.getMaxFrameLength(), true));
    // add CORS handler
    if (config.isCorsEnabled()) {
      pipeline.addLast(corsHeadersHandler);
    }
    // message acceptor
    pipeline.addLast(messageHandler);
    // at-least-something exception handler
    pipeline.addLast(new ChannelInboundHandlerAdapter() {
      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.warn("Exception caught for channel {}, {}", ctx.channel(), cause.getMessage(), cause);
      }
    });
  }
}
