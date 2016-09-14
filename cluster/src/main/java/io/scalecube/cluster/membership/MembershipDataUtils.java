package io.scalecube.cluster.membership;

import io.scalecube.transport.Message;
import io.scalecube.transport.Address;

import java.util.Collection;
import java.util.stream.Collectors;

import rx.functions.Func1;

final class MembershipDataUtils {

  private MembershipDataUtils() {}

  /**
   * Creates new {@link MembershipData} based on given {@code data} with excluding record corresponding to given
   * {@code localAddress}.
   */
  public static MembershipData filterData(final Address localAddress, MembershipData data) {
    // TODO: why?
    Collection<MembershipRecord> filteredData = data.getMembership().stream()
        .filter(membershipRecord -> !localAddress.equals(membershipRecord.address()))
        .collect(Collectors.toList());
    return new MembershipData(filteredData, data.getSyncGroup());
  }

  /**
   * In the incoming {@code transportMessage} filters {@link MembershipData} by excluding record with
   * {@code localAddress}.
   */
  static Func1<Message, MembershipData> gossipFilterData(final Address localAddress) {
    return gossip -> filterData(localAddress, gossip.data());
  }

}
