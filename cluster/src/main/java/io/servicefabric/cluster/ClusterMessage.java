package io.servicefabric.cluster;

import javax.annotation.CheckForNull;
import javax.annotation.concurrent.Immutable;

import io.servicefabric.transport.protocol.Message;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author Anton Kharenko
 */
@Immutable
public class ClusterMessage {

  private final Message message;
  private final ClusterMember member;

  public ClusterMessage(@CheckForNull Message message, @CheckForNull ClusterMember member) {
    checkArgument(member != null);
    checkArgument(message != null);
    this.message = message;
    this.member = member;
  }

  public Message message() {
    return message;
  }

  public ClusterMember member() {
    return member;
  }

  @Override
  public String toString() {
    return "ClusterMember{" +
        "message=" + message +
        ", member=" + member +
        '}';
  }

}
