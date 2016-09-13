package io.scalecube.cluster.membership;

import static io.scalecube.cluster.membership.MemberStatus.REMOVED;
import static io.scalecube.cluster.membership.MemberStatus.SUSPECTED;
import static io.scalecube.cluster.membership.MemberStatus.TRUSTED;

import io.scalecube.cluster.fdetector.FailureDetectorEvent;
import io.scalecube.transport.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class MembershipTable {

  private final ConcurrentMap<String, MembershipRecord> records = new ConcurrentHashMap<>();

  public List<MembershipRecord> merge(MembershipData data) {
    List<MembershipRecord> updates = new ArrayList<>();
    for (MembershipRecord record : data.getMembership()) {
      updates.addAll(merge(record));
    }
    return updates;
  }

  public List<MembershipRecord> merge(MembershipRecord r1) {
    List<MembershipRecord> updates = new ArrayList<>(1);
    MembershipRecord r0 = records.putIfAbsent(r1.id(), r1);
    if (r0 == null) {
      updates.add(r1);
    } else if (r0.compareTo(r1) < 0) {
      if (records.replace(r1.id(), r0, r1)) {
        updates.add(r1);
      } else {
        return merge(r1);
      }
    }
    return updates;
  }

  public List<MembershipRecord> merge(FailureDetectorEvent event) {
    MembershipRecord r0 = get(event.address());
    if (r0 != null) {
      return merge(new MembershipRecord(r0.member(), event.status()));
    } else {
      return Collections.emptyList();
    }
  }

  public MembershipRecord get(Address address) {
    // TODO [AK]: Temporary solution, should be optimized!!!
    for (MembershipRecord member : records.values()) {
      if (member.address().equals(address)) {
        return member;
      }
    }
    return null;
  }

  public MembershipRecord get(String id) {
    return records.get(id);
  }

  public List<MembershipRecord> remove(String id) {
    List<MembershipRecord> updates = new ArrayList<>(1);
    MembershipRecord r0 = records.remove(id);
    if (r0 != null) {
      updates.add(new MembershipRecord(r0.member(), REMOVED));
    }
    return updates;
  }

  public List<MembershipRecord> asList() {
    return new ArrayList<>(records.values());
  }

  /**
   * Getting {@code TRUSTED} or {@code SUSPECTED} member's addresses.
   */
  public Collection<Address> getTrustedOrSuspectedMembers() {
    Collection<Address> addresses = new ArrayList<>();
    for (MembershipRecord member : records.values()) {
      if (member.status() == TRUSTED || member.status() == SUSPECTED) {
        addresses.add(member.address());
      }
    }
    return addresses;
  }
}
