package io.servicefabric.transport;

import static com.google.common.base.Preconditions.checkArgument;

import io.servicefabric.transport.protocol.Message;

import javax.annotation.CheckForNull;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class TransportMessage {

  private final Message message;
  private final TransportEndpoint endpoint;

  public TransportMessage(@CheckForNull Message message, @CheckForNull TransportEndpoint endpoint) {
    checkArgument(endpoint != null);
    checkArgument(message != null);
    this.message = message;
    this.endpoint = endpoint;
  }

  public Message message() {
    return message;
  }

  public TransportEndpoint endpoint() {
    return endpoint;
  }

  @Override
  public String toString() {
    return "TransportMessage{" +
        "message=" + message +
        ", endpoint=" + endpoint +
        '}';
  }
}
