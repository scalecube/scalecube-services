package io.servicefabric.cluster;

import static com.google.common.collect.Collections2.filter;

import io.servicefabric.transport.TransportEndpoint;
import io.servicefabric.transport.Message;

import com.google.common.base.Predicate;

import rx.functions.Func1;

final class ClusterMembershipDataUtils {

  private ClusterMembershipDataUtils() {}

  /**
   * Creates new {@link ClusterMembershipData} based on given {@code data} with excluding record corresponding to given
   * {@code localEndpoint}.
   */
  public static ClusterMembershipData filterData(final TransportEndpoint localEndpoint, ClusterMembershipData data) {
    return new ClusterMembershipData(filter(data.getMembership(), new Predicate<ClusterMember>() {
      @Override
      public boolean apply(ClusterMember input) {
        return !localEndpoint.equals(input.endpoint());
      }
    }), data.getSyncGroup());
  }

  /**
   * In the incoming {@code transportMessage} filters {@link ClusterMembershipData} by excluding record with
   * {@code localEndpoint}.
   */
  static Func1<Message, ClusterMembershipData> gossipFilterData(final TransportEndpoint localEndpoint) {
    return new Func1<Message, ClusterMembershipData>() {
      @Override
      public ClusterMembershipData call(Message gossip) {
        ClusterMembershipData data = gossip.data();
        return filterData(localEndpoint, data);
      }
    };
  }

  /**
   * Filter function. Checking cluster identifier. See {@link ClusterMembershipData#syncGroup}.
   */
  static Func1<Message, Boolean> syncGroupFilter(final String syncGroup) {
    return new Func1<Message, Boolean>() {
      @Override
      public Boolean call(Message message) {
        ClusterMembershipData data = message.data();
        return syncGroup.equals(data.getSyncGroup());
      }
    };
  }

}
