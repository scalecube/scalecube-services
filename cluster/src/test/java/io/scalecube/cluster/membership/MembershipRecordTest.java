package io.scalecube.cluster.membership;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.HashMap;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;

public class MembershipRecordTest {

  private final Member member0 = new Member("id0", Address.from("localhost:1"), new HashMap<String, String>());
  private final Member member1 = new Member("id1", Address.from("localhost:2"), new HashMap<String, String>());

  @Test
  public void testCompareSameStatus() {
    assertEquals(0, r0(MemberStatus.SUSPECTED).compareTo(r1(MemberStatus.SUSPECTED)));
    assertEquals(0, r0(MemberStatus.TRUSTED).compareTo(r1(MemberStatus.TRUSTED)));
    assertEquals(0, r0(MemberStatus.REMOVED).compareTo(r1(MemberStatus.REMOVED)));
    assertEquals(0, r0(MemberStatus.SHUTDOWN).compareTo(r1(MemberStatus.SHUTDOWN)));
  }

  @Test
  public void testCompareShutdown() throws Exception {
    assertEquals(1, r0(MemberStatus.SHUTDOWN).compareTo(r1(MemberStatus.TRUSTED)));
    assertEquals(1, r0(MemberStatus.SHUTDOWN).compareTo(r1(MemberStatus.SUSPECTED)));
    assertEquals(1, r0(MemberStatus.SHUTDOWN).compareTo(r1(MemberStatus.REMOVED)));

    assertEquals(-1, r0(MemberStatus.TRUSTED).compareTo(r1(MemberStatus.SHUTDOWN)));
    assertEquals(-1, r0(MemberStatus.SUSPECTED).compareTo(r1(MemberStatus.SHUTDOWN)));
    assertEquals(-1, r0(MemberStatus.REMOVED).compareTo(r1(MemberStatus.SHUTDOWN)));
  }

  @Test
  public void testCompareWithTimestamp() {
    assertEquals(1, r0(MemberStatus.SUSPECTED, 1).compareTo(r1(MemberStatus.TRUSTED, 1)));
    assertEquals(-1, r0(MemberStatus.TRUSTED, 1).compareTo(r1(MemberStatus.SUSPECTED, 1)));

    assertEquals(-1, r0(MemberStatus.SUSPECTED, 1).compareTo(r1(MemberStatus.TRUSTED, 2)));
    assertEquals(1, r0(MemberStatus.SUSPECTED, 2).compareTo(r1(MemberStatus.TRUSTED, 1)));

    assertEquals(-1, r0(MemberStatus.TRUSTED, 1).compareTo(r1(MemberStatus.SUSPECTED, 2)));
    assertEquals(1, r0(MemberStatus.TRUSTED, 2).compareTo(r1(MemberStatus.SUSPECTED, 1)));
  }

  private MembershipRecord r0(MemberStatus status) {
    return new MembershipRecord(member0, status);
  }

  private MembershipRecord r1(MemberStatus status) {
    return new MembershipRecord(member1, status);
  }

  private MembershipRecord r0(MemberStatus status, long timestamp) {
    return new MembershipRecord(member0, status, timestamp);
  }

  private MembershipRecord r1(MemberStatus status, long timestamp) {
    return new MembershipRecord(member1, status, timestamp);
  }
}
