package io.scalecube.cluster;

import static io.scalecube.transport.TransportEndpoint.from;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.HashMap;

public class ClusterMemberTest {

  @Test
  public void testCompareSameStatus() {
    assertEquals(0, r0(ClusterMemberStatus.SUSPECTED).compareTo(r1(ClusterMemberStatus.SUSPECTED)));
    assertEquals(0, r0(ClusterMemberStatus.TRUSTED).compareTo(r1(ClusterMemberStatus.TRUSTED)));
    assertEquals(0, r0(ClusterMemberStatus.REMOVED).compareTo(r1(ClusterMemberStatus.REMOVED)));
    assertEquals(0, r0(ClusterMemberStatus.SHUTDOWN).compareTo(r1(ClusterMemberStatus.SHUTDOWN)));
  }

  @Test
  public void testCompareShutdown() throws Exception {
    assertEquals(1, r0(ClusterMemberStatus.SHUTDOWN).compareTo(r1(ClusterMemberStatus.TRUSTED)));
    assertEquals(1, r0(ClusterMemberStatus.SHUTDOWN).compareTo(r1(ClusterMemberStatus.SUSPECTED)));
    assertEquals(1, r0(ClusterMemberStatus.SHUTDOWN).compareTo(r1(ClusterMemberStatus.REMOVED)));

    assertEquals(-1, r0(ClusterMemberStatus.TRUSTED).compareTo(r1(ClusterMemberStatus.SHUTDOWN)));
    assertEquals(-1, r0(ClusterMemberStatus.SUSPECTED).compareTo(r1(ClusterMemberStatus.SHUTDOWN)));
    assertEquals(-1, r0(ClusterMemberStatus.REMOVED).compareTo(r1(ClusterMemberStatus.SHUTDOWN)));
  }

  @Test
  public void testCompareWithTimestamp() {
    assertEquals(1, r0(ClusterMemberStatus.SUSPECTED, 1).compareTo(r1(ClusterMemberStatus.TRUSTED, 1)));
    assertEquals(-1, r0(ClusterMemberStatus.TRUSTED, 1).compareTo(r1(ClusterMemberStatus.SUSPECTED, 1)));

    assertEquals(-1, r0(ClusterMemberStatus.SUSPECTED, 1).compareTo(r1(ClusterMemberStatus.TRUSTED, 2)));
    assertEquals(1, r0(ClusterMemberStatus.SUSPECTED, 2).compareTo(r1(ClusterMemberStatus.TRUSTED, 1)));

    assertEquals(-1, r0(ClusterMemberStatus.TRUSTED, 1).compareTo(r1(ClusterMemberStatus.SUSPECTED, 2)));
    assertEquals(1, r0(ClusterMemberStatus.TRUSTED, 2).compareTo(r1(ClusterMemberStatus.SUSPECTED, 1)));
  }

  private ClusterMember r0(ClusterMemberStatus status) {
    return new ClusterMember("id0", from("localhost:1:id0"), status, new HashMap<String, String>());
  }

  private ClusterMember r1(ClusterMemberStatus status) {
    return new ClusterMember("id1", from("localhost:2:id1"), status, new HashMap<String, String>());
  }

  private ClusterMember r0(ClusterMemberStatus status, long timestamp) {
    return new ClusterMember("id0", from("localhost:1:id0"), status, new HashMap<String, String>(), timestamp);
  }

  private ClusterMember r1(ClusterMemberStatus status, long timestamp) {
    return new ClusterMember("id1", from("localhost:2:id1"), status, new HashMap<String, String>(), timestamp);
  }
}
