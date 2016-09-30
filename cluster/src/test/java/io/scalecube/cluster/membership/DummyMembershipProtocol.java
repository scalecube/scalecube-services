package io.scalecube.cluster.membership;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;
import rx.Observable;

/**
 * @author Anton Kharenko
 */
public class DummyMembershipProtocol implements IMembershipProtocol {

  private Member localMember;
  private List<Member> remoteMembers = new ArrayList<>();

  public DummyMembershipProtocol(Address localAddress, List<Address> allAddresses) {
    int count = 0;
    for (Address address : allAddresses) {
      Member member = new Member(Integer.toString(count++), address);
      if (address.equals(localAddress)) {
        localMember = member;
      } else {
        remoteMembers.add(member);
      }
    }
  }

  @Override
  public Member member() {
    return localMember;
  }

  @Override
  public Observable<MembershipEvent> listen() {
    return Observable.from(remoteMembers.stream()
        .map(member -> new MembershipEvent(MembershipEvent.Type.ADDED, member))
        .collect(Collectors.toList()));
  }
}
