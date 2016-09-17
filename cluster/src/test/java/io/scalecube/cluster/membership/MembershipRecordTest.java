package io.scalecube.cluster.membership;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.HashMap;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;

public class MembershipRecordTest {

  // TODO: write tests for override method

  /*
  private final Member member0 = new Member("id0", Address.from("localhost:1"), new HashMap<String, String>());
  private final Member member1 = new Member("id1", Address.from("localhost:2"), new HashMap<String, String>());

  @Test
  public void testCompareSameStatus() {
    assertEquals(0, r0(MemberStatus.SUSPECT).compareTo(r1(MemberStatus.SUSPECT)));
    assertEquals(0, r0(MemberStatus.ALIVE).compareTo(r1(MemberStatus.ALIVE)));
    assertEquals(0, r0(MemberStatus.DEAD).compareTo(r1(MemberStatus.DEAD)));
  }

  @Test
  public void testCompareWithTimestamp() {
    assertEquals(1, r0(MemberStatus.SUSPECT, 1).compareTo(r1(MemberStatus.ALIVE, 1)));
    assertEquals(-1, r0(MemberStatus.ALIVE, 1).compareTo(r1(MemberStatus.SUSPECT, 1)));

    assertEquals(-1, r0(MemberStatus.SUSPECT, 1).compareTo(r1(MemberStatus.ALIVE, 2)));
    assertEquals(1, r0(MemberStatus.SUSPECT, 2).compareTo(r1(MemberStatus.ALIVE, 1)));

    assertEquals(-1, r0(MemberStatus.ALIVE, 1).compareTo(r1(MemberStatus.SUSPECT, 2)));
    assertEquals(1, r0(MemberStatus.ALIVE, 2).compareTo(r1(MemberStatus.SUSPECT, 1)));
  }

  private MembershipRecord r0(MemberStatus status) {
    return new MembershipRecord(member0, status, 0);
  }

  private MembershipRecord r1(MemberStatus status) {
    return new MembershipRecord(member1, status, 0);
  }

  private MembershipRecord r0(MemberStatus status, long timestamp) {
    return new MembershipRecord(member0, status, timestamp);
  }

  private MembershipRecord r1(MemberStatus status, long timestamp) {
    return new MembershipRecord(member1, status, timestamp);
  }
  */
}
