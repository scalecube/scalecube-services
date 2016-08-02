package io.scalecube.transport;

import io.netty.channel.Channel;
import io.netty.handler.logging.LogLevel;
import io.netty.util.concurrent.EventExecutorGroup;

/** Transport SPI interface. Exposes some properties and functions which aren't part of the public API. */
interface ITransportSpi extends ITransport {

  /** SPI method. */
  void onMessage(Message message);

  /** SPI method. */
  int getHandshakeTimeout();

  /** SPI method. */
  int getSendHighWaterMark();

  /** SPI method. */
  LogLevel getLogLevel();

  /** SPI method. */
  EventExecutorGroup getEventExecutor();

  /** SPI method. */
  void resetDueHandshake(Channel channel);
}
