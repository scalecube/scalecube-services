package io.scalecube.cluster.fdetector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.transport.Address;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportConfig;

import com.google.common.util.concurrent.SettableFuture;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FailureDetectorIT {

  @Test
  public void testBasicTrusted() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b);

    try {
      startAll(fdetectors);
      TimeUnit.SECONDS.sleep(2);
      assertTrue("No suspected members is expected by a", fd_a.getSuspectedMembers().isEmpty());
      assertTrue("No suspected members is expected by b", fd_b.getSuspectedMembers().isEmpty());
    } finally {
      stopAll(fdetectors);
      destroyTransports(a, b);
    }
  }

  @Test
  public void testAllTrusted() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    Transport e = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), d.address(), e.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_c = createFailureDetector(c, members);
    FailureDetector fd_d = createFailureDetector(d, members);
    FailureDetector fd_e = createFailureDetector(e, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c, fd_d, fd_e);

    try {
      startAll(fdetectors);
      TimeUnit.SECONDS.sleep(2);
      assertTrue("No suspected members is expected by a", fd_a.getSuspectedMembers().isEmpty());
      assertTrue("No suspected members is expected by b", fd_b.getSuspectedMembers().isEmpty());
      assertTrue("No suspected members is expected by c", fd_c.getSuspectedMembers().isEmpty());
      assertTrue("No suspected members is expected by d", fd_d.getSuspectedMembers().isEmpty());
      assertTrue("No suspected members is expected by e", fd_e.getSuspectedMembers().isEmpty());
    } finally {
      stopAll(fdetectors);
      destroyTransports(a, b, c, d, e);
    }
  }

  @Test
  public void testBasicSuspected() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b);

    // block all traffic
    a.block(members);
    b.block(members);

    try {
      startAll(fdetectors);
      TimeUnit.SECONDS.sleep(2);
      assertSuspected(fd_a.getSuspectedMembers(), b.address());
      assertSuspected(fd_b.getSuspectedMembers(), a.address());
    } finally {
      stopAll(fdetectors);
      destroyTransports(a, b);
    }
  }

  @Test
  public void testAllSuspected() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_c = createFailureDetector(c, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    // block all traffic
    a.block(members);
    b.block(members);
    c.block(members);

    try {
      startAll(fdetectors);
      TimeUnit.SECONDS.sleep(4);
      assertSuspected(fd_a.getSuspectedMembers(), b.address(), c.address());
      assertSuspected(fd_b.getSuspectedMembers(), a.address(), c.address());
      assertSuspected(fd_c.getSuspectedMembers(), a.address(), b.address());
    } finally {
      stopAll(fdetectors);
      destroyTransports(a, b, c);
    }
  }

  @Test
  public void testAllTrustedDespiteTrafficIssue() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_c = createFailureDetector(c, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c);

    // Traffic issue
    a.block(b.address());

    try {
      startAll(fdetectors);
      TimeUnit.SECONDS.sleep(4);
      assertTrue("No suspected members is expected by a", fd_a.getSuspectedMembers().isEmpty());
      assertTrue("No suspected members is expected by b", fd_b.getSuspectedMembers().isEmpty());
      assertTrue("No suspected members is expected by c", fd_c.getSuspectedMembers().isEmpty());
    } finally {
      stopAll(fdetectors);
      destroyTransports(a, b, c);
    }
  }

  @Test
  public void testTrustedDifferentPingTiming() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetectorConfig fd_b_config = FailureDetectorConfig.builder()
        .pingTime(500)
        .pingTimeout(300)
        .build();
    FailureDetector fd_b = createFailureDetector(b, members, fd_b_config);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b);

    try {
      startAll(fdetectors);
      TimeUnit.SECONDS.sleep(4);
      assertNoSuspected(fdetectors);
    } finally {
      stopAll(fdetectors);
      destroyTransports(a, b);
    }
  }

  @Test
  public void testSingleSuspectedNotAffectOthers() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), d.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_c = createFailureDetector(c, members);
    FailureDetector fd_d = createFailureDetector(d, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c, fd_d);

    // Block member a
    a.block(members);

    try {
      startAll(fdetectors);
      TimeUnit.SECONDS.sleep(4);
      assertSuspected(fd_a.getSuspectedMembers(), b.address(), c.address(), d.address());
      assertSuspected(fd_b.getSuspectedMembers(), a.address());
      assertSuspected(fd_c.getSuspectedMembers(), a.address());
      assertSuspected(fd_d.getSuspectedMembers(), a.address());

      // Unblock traffic
      a.unblockAll();

      // Check recovers
      TimeUnit.SECONDS.sleep(4);
      assertNoSuspected(fdetectors);
    } finally {
      stopAll(fdetectors);
      destroyTransports(a, b, c, d);
    }
  }

  @Test
  public void testTwoSuspectedNotAffectOthers() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport d = Transport.bindAwait(true);
    Transport e = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), d.address(), e.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_c = createFailureDetector(c, members);
    FailureDetector fd_d = createFailureDetector(d, members);
    FailureDetector fd_e = createFailureDetector(e, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c, fd_d, fd_e);

    // Block two members a & b
    a.block(members);
    b.block(members);

    try {
      startAll(fdetectors);
      TimeUnit.SECONDS.sleep(6);
      assertSuspected(fd_a.getSuspectedMembers(), b.address(), c.address(), d.address(), e.address());
      assertSuspected(fd_b.getSuspectedMembers(), a.address(), c.address(), d.address(), e.address());
      assertSuspected(fd_c.getSuspectedMembers(), a.address(), b.address());
      assertSuspected(fd_d.getSuspectedMembers(), a.address(), b.address());
      assertSuspected(fd_e.getSuspectedMembers(), a.address(), b.address());

      // Unblock traffic
      a.unblockAll();
      b.unblockAll();

      // Check recovers
      TimeUnit.SECONDS.sleep(6);
      assertNoSuspected(fdetectors);
    } finally {
      stopAll(fdetectors);
      destroyTransports(a, b, c, d, e);
    }
  }

  @Test
  public void testSuspectedNetworkPartition() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport c = Transport.bindAwait(true);
    Transport x = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), c.address(), x.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_c = createFailureDetector(c, members);
    FailureDetector fd_x = createFailureDetector(x, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_c, fd_x);

    // Block traffic to x
    a.block(x.address());
    b.block(x.address());
    c.block(x.address());

    try {
      startAll(fdetectors);
      TimeUnit.SECONDS.sleep(4);
      assertSuspected(fd_x.getSuspectedMembers(), a.address(), b.address(), c.address());
      assertSuspected(fd_a.getSuspectedMembers(), x.address());
      assertSuspected(fd_b.getSuspectedMembers(), x.address());
      assertSuspected(fd_c.getSuspectedMembers(), x.address());
    } finally {
      stopAll(fdetectors);
      destroyTransports(a, b, c, x);
    }
  }

  @Test
  public void testMemberBecomeTrusted() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b);

    // traffic is blocked initially
    a.block(b.address());
    b.block(a.address());

    try {
      startAll(fdetectors);
      TimeUnit.SECONDS.sleep(4);
      assertSuspected(fd_a.getSuspectedMembers(), b.address());
      assertSuspected(fd_b.getSuspectedMembers(), a.address());

      // unblock all traffic
      a.unblockAll();
      b.unblockAll();

      TimeUnit.SECONDS.sleep(4);
      assertNoSuspected(fdetectors);
    } finally {
      stopAll(fdetectors);
      destroyTransports(a, b);
    }
  }

  @Test
  public void testMemberBecomeSuspected() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport x = Transport.bindAwait(true);
    Transport y = Transport.bindAwait(true);
    List<Address> members = Arrays.asList(a.address(), b.address(), x.address(), y.address());

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_x = createFailureDetector(x, members);
    FailureDetector fd_y = createFailureDetector(y, members);
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_x, fd_y);

    try {
      startAll(fdetectors);

      TimeUnit.SECONDS.sleep(2);
      assertNoSuspected(fdetectors);

      // Destroy x and y
      fd_x.stop();
      fd_y.stop();
      destroyTransports(x, y);

      TimeUnit.SECONDS.sleep(4);

      assertSuspected(fd_a.getSuspectedMembers(), x.address(), y.address());
      assertSuspected(fd_b.getSuspectedMembers(), x.address(), y.address());
    } finally {
      stopAll(fdetectors);
      destroyTransports(a, b, x, y);
    }
  }

  @Test
  public void testMemberBecomeTrustedAfterRestart() throws Exception {
    // Create transports
    Transport a = Transport.bindAwait(true);
    Transport b = Transport.bindAwait(true);
    Transport x = Transport.bindAwait(true);
    Transport xx = null;
    List<Address> members = new ArrayList<>(Arrays.asList(a.address(), b.address(), x.address()));

    // Create failure detectors
    FailureDetector fd_a = createFailureDetector(a, members);
    FailureDetector fd_b = createFailureDetector(b, members);
    FailureDetector fd_x = createFailureDetector(x, members);
    FailureDetector fd_xx = null;
    List<FailureDetector> fdetectors = Arrays.asList(fd_a, fd_b, fd_x);

    try {
      startAll(fdetectors);
      TimeUnit.SECONDS.sleep(2);
      assertNoSuspected(fdetectors);

      // stop x
      fd_x.stop();
      destroyTransport(x);

      TimeUnit.SECONDS.sleep(4);
      assertSuspected(fd_a.getSuspectedMembers(), x.address());
      assertSuspected(fd_b.getSuspectedMembers(), x.address());

      // restart x (as xx)
      TransportConfig xx_config = TransportConfig.builder()
          .port(x.address().port())
          .portAutoIncrement(false)
          .useNetworkEmulator(true)
          .build();
      xx = Transport.bindAwait(xx_config);
      assertEquals(x.address(), xx.address());
      fd_xx = createFailureDetector(xx, members);
      fd_xx.start();

      TimeUnit.SECONDS.sleep(4);
      // TODO [AK]: It would be more correct to consider restarted member as a new member, so x is still suspected!
      assertNoSuspected(fdetectors);
    } finally {
      stopAll(fdetectors);
      fd_xx.stop();
      destroyTransports(a, b, x, xx);
    }
  }

  private FailureDetector createFailureDetector(Transport transport, List<Address> members) {
    FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.builder() // faster config for local testing
        .pingTimeout(100)
        .pingTime(200)
        .maxMembersToSelect(2)
        .build();
    FailureDetector failureDetector = new FailureDetector(transport, failureDetectorConfig);
    failureDetector.setMembers(members);
    return createFailureDetector(transport, members, failureDetectorConfig);
  }

  private FailureDetector createFailureDetector(Transport transport, List<Address> members,
      FailureDetectorConfig failureDetectorConfig) {
    FailureDetector failureDetector = new FailureDetector(transport, failureDetectorConfig);
    failureDetector.setMembers(members);
    return failureDetector;
  }

  private void destroyTransports(Transport... transports) {
    for (Transport transport : transports) {
      destroyTransport(transport);
    }
  }

  private void destroyTransport(Transport transport) {
    if (transport != null && !transport.isStopped()) {
      SettableFuture<Void> close = SettableFuture.create();
      transport.stop(close);
      try {
        close.get(1, TimeUnit.SECONDS);
      } catch (Exception ignore) {
        // ignore
      }
    }
  }

  private void assertSuspected(List<Address> actual, Address... expected) {
    assertEquals("Expected " + expected.length + " suspected members " + Arrays.toString(expected)
        + ", but actual: " + actual, expected.length, actual.size());
    for (Address member : expected) {
      assertTrue("Expected to suspect " + member + ", but actual: " + actual, actual.contains(member));
    }
  }

  private void assertNoSuspected(List<FailureDetector> fdetectors) {
    for (FailureDetector fd : fdetectors) {
      List<Address> suspectMembers = fd.getSuspectedMembers();
      assertTrue("No suspected members is expected, but: " + suspectMembers, suspectMembers.isEmpty());
    }
  }

  private void startAll(List<FailureDetector> fdetectors) {
    for (FailureDetector fd : fdetectors) {
      fd.start();
    }
  }

  private void stopAll(List<FailureDetector> fdetectors) {
    for (FailureDetector fd : fdetectors) {
      fd.stop();
    }
  }

}
