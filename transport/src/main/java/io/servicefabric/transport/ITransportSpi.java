package io.servicefabric.transport;

import io.netty.channel.Channel;
import io.netty.handler.logging.LogLevel;
import io.netty.util.concurrent.EventExecutorGroup;

import rx.subjects.Subject;

import java.util.Map;

/** Transport SPI interface. Exposes some properties and functions which aren't part of the public API. */
interface ITransportSpi {

  /** SPI method. */
  void onMessage(TransportMessage message);

  /** SPI method. */
  TransportEndpoint getLocalEndpoint();

  /** SPI method. */
  int getHandshakeTimeout();

  /** SPI method. */
  int getSendHighWaterMark();

  /** SPI method. */
  LogLevel getLogLevel();

  /** SPI method. */
  EventExecutorGroup getEventExecutor();

  /** SPI method. */
  TransportChannel createAcceptorTransportChannel(Channel channel);

  /** SPI method. */
  void accept(TransportChannel transport);

  /** SPI method. */
  void resetDueHandshake(Channel channel);
}
