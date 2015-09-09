package io.servicefabric.cluster;

import static com.google.common.collect.Collections2.filter;

import io.servicefabric.transport.TransportMessage;
import io.servicefabric.transport.protocol.Message;

import com.google.common.base.Predicate;

import rx.functions.Func1;

final class ClusterMembershipDataUtils {

  private ClusterMembershipDataUtils() {}

  /**
   * In the incoming {@code transportMessage} filters {@link ClusterMembershipData} by excluding record with {@code localEndpoint}.
   */
  static Func1<TransportMessage, TransportMessage> filterData(final ClusterEndpoint localEndpoint) {
    return new Func1<TransportMessage, TransportMessage>() {
      @Override
      public TransportMessage call(TransportMessage transportMessage) {
        Message message = transportMessage.message();
        ClusterMembershipData filteredData = filterData(localEndpoint, (ClusterMembershipData) message.data());
        Message filteredMessage = new Message(filteredData, message.headers());
        return new TransportMessage(transportMessage.originChannel(), filteredMessage, transportMessage.originEndpoint(),
            transportMessage.originEndpointId());
      }
    };
  }

  private static ClusterMembershipData filterData(final ClusterEndpoint localEndpoint, ClusterMembershipData data) {
    return new ClusterMembershipData(filter(data.getMembership(), new Predicate<ClusterMember>() {
      @Override
      public boolean apply(ClusterMember input) {
        return !localEndpoint.equals(input.endpoint());
      }
    }), data.getSyncGroup());
  }

  /**
   * In the incoming {@code transportMessage} filters {@link ClusterMembershipData} by excluding record with {@code localEndpoint}.
   */
  static Func1<Message, ClusterMembershipData> gossipFilterData(final ClusterEndpoint localEndpoint) {
    return new Func1<Message, ClusterMembershipData>() {
      @Override
      public ClusterMembershipData call(Message gossip) {
        return filterData(localEndpoint, (ClusterMembershipData) gossip.data());
      }
    };
  }

  /**
   * Filter function. Checking cluster identifier. See {@link ClusterMembershipData#syncGroup}.
   */
  static Func1<TransportMessage, Boolean> syncGroupFilter(final String syncGroup) {
    return new Func1<TransportMessage, Boolean>() {
      @Override
      public Boolean call(TransportMessage transportMessage) {
        ClusterMembershipData data = (ClusterMembershipData) transportMessage.message().data();
        return syncGroup.equals(data.getSyncGroup());
      }
    };
  }

}
