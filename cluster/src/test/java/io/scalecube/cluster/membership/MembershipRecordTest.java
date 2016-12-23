package io.scalecube.cluster.membership;

import org.junit.Test;

import io.scalecube.cluster.Member;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Address;

import static io.scalecube.cluster.membership.MemberStatus.ALIVE;
import static io.scalecube.cluster.membership.MemberStatus.DEAD;
import static io.scalecube.cluster.membership.MemberStatus.SUSPECT;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class MembershipRecordTest extends BaseTest {

  private final Member member = new Member("0", Address.from("localhost:1234"));
  private final Member anotherMember = new Member("1", Address.from("localhost:4567"));

  private final MembershipRecord r0_null = null;

  private final MembershipRecord r0_alive_0 = new MembershipRecord(member, ALIVE, 0);
  private final MembershipRecord r0_alive_1 = new MembershipRecord(member, ALIVE, 1);
  private final MembershipRecord r0_alive_2 = new MembershipRecord(member, ALIVE, 2);

  private final MembershipRecord r0_suspect_0 = new MembershipRecord(member, SUSPECT, 0);
  private final MembershipRecord r0_suspect_1 = new MembershipRecord(member, SUSPECT, 1);
  private final MembershipRecord r0_suspect_2 = new MembershipRecord(member, SUSPECT, 2);

  private final MembershipRecord r0_dead_0 = new MembershipRecord(member, DEAD, 0);
  private final MembershipRecord r0_dead_1 = new MembershipRecord(member, DEAD, 1);
  private final MembershipRecord r0_dead_2 = new MembershipRecord(member, DEAD, 2);

  @Test(expected = IllegalArgumentException.class)
  public void testCantCompareDifferentMembers() {
    MembershipRecord r0 = new MembershipRecord(member, ALIVE, 0);
    MembershipRecord r1 = new MembershipRecord(anotherMember, ALIVE, 0);

    r1.isOverrides(r0); // throws exception
  }

  @Test
  public void testDeadOverride() {
    MembershipRecord r1_dead_1 = new MembershipRecord(member, DEAD, 1);

    assertFalse(r1_dead_1.isOverrides(r0_null));

    assertTrue(r1_dead_1.isOverrides(r0_alive_0));
    assertTrue(r1_dead_1.isOverrides(r0_alive_1));
    assertTrue(r1_dead_1.isOverrides(r0_alive_2));

    assertTrue(r1_dead_1.isOverrides(r0_suspect_0));
    assertTrue(r1_dead_1.isOverrides(r0_suspect_1));
    assertTrue(r1_dead_1.isOverrides(r0_suspect_2));

    assertFalse(r1_dead_1.isOverrides(r0_dead_0));
    assertFalse(r1_dead_1.isOverrides(r0_dead_1));
    assertFalse(r1_dead_1.isOverrides(r0_dead_2));
  }

  @Test
  public void testAliveOverride() {
    MembershipRecord r1_alive_1 = new MembershipRecord(member, ALIVE, 1);

    assertTrue(r1_alive_1.isOverrides(r0_null));

    assertTrue(r1_alive_1.isOverrides(r0_alive_0));
    assertFalse(r1_alive_1.isOverrides(r0_alive_1));
    assertFalse(r1_alive_1.isOverrides(r0_alive_2));

    assertTrue(r1_alive_1.isOverrides(r0_suspect_0));
    assertFalse(r1_alive_1.isOverrides(r0_suspect_1));
    assertFalse(r1_alive_1.isOverrides(r0_suspect_2));

    assertFalse(r1_alive_1.isOverrides(r0_dead_0));
    assertFalse(r1_alive_1.isOverrides(r0_dead_1));
    assertFalse(r1_alive_1.isOverrides(r0_dead_2));
  }

  @Test
  public void testSuspectOverride() {
    MembershipRecord r1_suspect_1 = new MembershipRecord(member, SUSPECT, 1);

    assertFalse(r1_suspect_1.isOverrides(r0_null));

    assertTrue(r1_suspect_1.isOverrides(r0_alive_0));
    assertTrue(r1_suspect_1.isOverrides(r0_alive_1));
    assertFalse(r1_suspect_1.isOverrides(r0_alive_2));

    assertTrue(r1_suspect_1.isOverrides(r0_suspect_0));
    assertFalse(r1_suspect_1.isOverrides(r0_suspect_1));
    assertFalse(r1_suspect_1.isOverrides(r0_suspect_2));

    assertFalse(r1_suspect_1.isOverrides(r0_dead_0));
    assertFalse(r1_suspect_1.isOverrides(r0_dead_1));
    assertFalse(r1_suspect_1.isOverrides(r0_dead_2));
  }

  @Test
  public void testEqualRecordNotOverriding() {
    assertFalse(r0_alive_1.isOverrides(r0_alive_1));
    assertFalse(r0_suspect_1.isOverrides(r0_suspect_1));
    assertFalse(r0_dead_1.isOverrides(r0_dead_1));
  }

}
