package io.scalecube.cluster.membership;

import static com.google.common.collect.Collections2.filter;

import io.scalecube.transport.Message;
import io.scalecube.transport.Address;

import com.google.common.base.Predicate;

import rx.functions.Func1;

final class MembershipDataUtils {

  private MembershipDataUtils() {}

  /**
   * Creates new {@link MembershipData} based on given {@code data} with excluding record corresponding to given
   * {@code localAddress}.
   */
  public static MembershipData filterData(final Address localAddress, MembershipData data) {
    return new MembershipData(filter(data.getMembership(), new Predicate<MembershipRecord>() {
      @Override
      public boolean apply(MembershipRecord input) {
        return !localAddress.equals(input.address());
      }
    }), data.getSyncGroup());
  }

  /**
   * In the incoming {@code transportMessage} filters {@link MembershipData} by excluding record with
   * {@code localAddress}.
   */
  static Func1<Message, MembershipData> gossipFilterData(final Address localAddress) {
    return new Func1<Message, MembershipData>() {
      @Override
      public MembershipData call(Message gossip) {
        MembershipData data = gossip.data();
        return filterData(localAddress, data);
      }
    };
  }

  /**
   * Filter function. Checking cluster identifier. See {@link MembershipData#syncGroup}.
   */
  static Func1<Message, Boolean> syncGroupFilter(final String syncGroup) {
    return new Func1<Message, Boolean>() {
      @Override
      public Boolean call(Message message) {
        MembershipData data = message.data();
        return syncGroup.equals(data.getSyncGroup());
      }
    };
  }

}
