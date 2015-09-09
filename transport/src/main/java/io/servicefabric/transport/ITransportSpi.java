package io.servicefabric.transport;

import io.netty.channel.Channel;
import io.netty.handler.logging.LogLevel;
import io.netty.util.concurrent.EventExecutorGroup;

import rx.subjects.Subject;

import java.util.Map;

/** Transport SPI interface. Exposes some properties and functions which aren't part of the public API. */
interface ITransportSpi {

  /** SPI method. */
  Subject<TransportMessage, TransportMessage> getSubject();

  /** SPI method. */
  TransportEndpoint getLocalEndpoint();

  /** SPI method. */
  Map<String, Object> getLocalMetadata();

  /** SPI method. */
  int getHandshakeTimeout();

  /** SPI method. */
  int getSendHwm();

  /** SPI method. */
  LogLevel getLogLevel();

  /** SPI method. */
  EventExecutorGroup getEventExecutor();

  /** SPI method. */
  TransportChannel getTransportChannel(Channel channel);

  /** SPI method. */
  TransportChannel createAcceptor(Channel channel);

  /** SPI method. */
  void accept(TransportChannel transport);

  /** SPI method. */
  void resetDueHandshake(Channel channel);
}
