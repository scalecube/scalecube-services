package io.scalecube.cluster.membership;

import static com.google.common.collect.Collections2.filter;

import io.scalecube.cluster.ClusterMember;
import io.scalecube.transport.Message;
import io.scalecube.transport.Address;

import com.google.common.base.Predicate;

import rx.functions.Func1;

final class ClusterMembershipDataUtils {

  private ClusterMembershipDataUtils() {}

  /**
   * Creates new {@link ClusterMembershipData} based on given {@code data} with excluding record corresponding to given
   * {@code localAddress}.
   */
  public static ClusterMembershipData filterData(final Address localAddress, ClusterMembershipData data) {
    return new ClusterMembershipData(filter(data.getMembership(), new Predicate<ClusterMember>() {
      @Override
      public boolean apply(ClusterMember input) {
        return !localAddress.equals(input.address());
      }
    }), data.getSyncGroup());
  }

  /**
   * In the incoming {@code transportMessage} filters {@link ClusterMembershipData} by excluding record with
   * {@code localAddress}.
   */
  static Func1<Message, ClusterMembershipData> gossipFilterData(final Address localAddress) {
    return new Func1<Message, ClusterMembershipData>() {
      @Override
      public ClusterMembershipData call(Message gossip) {
        ClusterMembershipData data = gossip.data();
        return filterData(localAddress, data);
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
