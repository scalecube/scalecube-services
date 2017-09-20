package io.scalecube.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.testlib.BaseTest;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClusterTest extends BaseTest {

  @Test
  public void testJoinDynamicPort() throws Exception {
    // Start seed node
    Cluster seedNode = Cluster.joinAwait();

    int membersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(membersNum);
    try {
      // Start other nodes
      long startAt = System.currentTimeMillis();
      for (int i = 0; i < membersNum; i++) {
        otherNodes.add(Cluster.joinAwait(seedNode.address()));
      }
      LOGGER.info("Start up time: {} ms", System.currentTimeMillis() - startAt);
      assertEquals(membersNum + 1, seedNode.members().size());
      LOGGER.info("Cluster nodes: {}", seedNode.members());
    } finally {
      // Shutdown all nodes
      shutdown(seedNode);
      shutdown(otherNodes);
    }
  }

  @Test
  public void testUpdateMetadata() throws Exception {
    // Start seed member
    Cluster seedNode = Cluster.joinAwait();

    Cluster metadataNode = null;
    int testMembersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(testMembersNum);
    try {
      // Start member with metadata
      Map<String, String> metadata = ImmutableMap.of("key1", "value1", "key2", "value2");
      metadataNode = Cluster.joinAwait(metadata, seedNode.address());

      // Start other test members
      for (int i = 0; i < testMembersNum; i++) {
        otherNodes.add(Cluster.joinAwait(seedNode.address()));
      }

      // Check all test members know valid metadata
      for (Cluster node : otherNodes) {
        Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
        assertTrue(mNodeOpt.isPresent());
        Member mNode = mNodeOpt.get();
        assertEquals(metadata, mNode.metadata());
      }

      // Subscribe for membership update event all nodes
      CountDownLatch updateLatch = new CountDownLatch(testMembersNum);
      for (Cluster node : otherNodes) {
        node.listenMembership()
            .filter(MembershipEvent::isUpdated)
            .subscribe(event -> {
              LOGGER.info("Received membership update event: {}", event);
              updateLatch.countDown();
            });
      }

      // Update metadata
      Map<String, String> updatedMetadata = ImmutableMap.of("key1", "value3");
      metadataNode.updateMetadata(updatedMetadata);

      // Await latch
      updateLatch.await(10, TimeUnit.SECONDS);

      // Check all nodes had updated metadata member
      for (Cluster node : otherNodes) {
        Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
        assertTrue(mNodeOpt.isPresent());
        Member mNode = mNodeOpt.get();
        assertEquals(updatedMetadata, mNode.metadata());
      }
    } finally {
      // Shutdown all nodes
      shutdown(seedNode);
      shutdown(metadataNode);
      shutdown(otherNodes);
    }
  }

  @Test
  public void testUpdateMetadataProperty() throws Exception {
    // Start seed member
    Cluster seedNode = Cluster.joinAwait();

    Cluster metadataNode = null;
    int testMembersNum = 10;
    List<Cluster> otherNodes = new ArrayList<>(testMembersNum);

    try {
      // Start member with metadata
      Map<String, String> metadata = ImmutableMap.of("key1", "value1", "key2", "value2");
      metadataNode = Cluster.joinAwait(metadata, seedNode.address());

      // Start other test members
      for (int i = 0; i < testMembersNum; i++) {
        otherNodes.add(Cluster.joinAwait(seedNode.address()));
      }

      // Check all test members know valid metadata
      for (Cluster node : otherNodes) {
        Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
        assertTrue(mNodeOpt.isPresent());
        Member mNode = mNodeOpt.get();
        assertEquals(metadata, mNode.metadata());
      }

      // Subscribe for membership update event all nodes
      CountDownLatch updateLatch = new CountDownLatch(testMembersNum);
      for (Cluster node : otherNodes) {
        node.listenMembership()
            .filter(MembershipEvent::isUpdated)
            .subscribe(event -> {
              LOGGER.info("Received membership update event: {}", event);
              updateLatch.countDown();
            });
      }

      // Update metadata
      metadataNode.updateMetadataProperty("key2", "value3");

      // Await latch
      updateLatch.await(10, TimeUnit.SECONDS);

      // Check all nodes had updated metadata member
      for (Cluster node : otherNodes) {
        Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
        assertTrue(mNodeOpt.isPresent());
        Member mNode = mNodeOpt.get();
        Map<String, String> mNodeMetadata = mNode.metadata();
        assertEquals(2, mNode.metadata().size());
        assertEquals("value1", mNodeMetadata.get("key1"));
        assertEquals("value3", mNodeMetadata.get("key2"));
      }
    } finally {
      // Shutdown all nodes
      shutdown(seedNode);
      shutdown(metadataNode);
      shutdown(otherNodes);
    }
  }

  @Test
  public void testShutdownCluster() throws Exception {
    // Start seed member
    Cluster seedNode = Cluster.joinAwait();

    // Start nodes
    Cluster node1 = Cluster.joinAwait(seedNode.address());
    Cluster node2 = Cluster.joinAwait(seedNode.address());
    Cluster node3 = Cluster.joinAwait(seedNode.address());

    CountDownLatch leave = new CountDownLatch(1);
    node2.shutdown().whenComplete((done, error) -> {
      leave.countDown();
    });

    leave.await(5, TimeUnit.SECONDS);
    assertTrue(!seedNode.members().contains(node2.member()));
    assertTrue(!node1.members().contains(node2.member()));
    assertTrue(!node3.members().contains(node2.member()));
    assertTrue(node2.isShutdown());

    seedNode.shutdown();
    node1.shutdown();
    node3.shutdown();
  }

  private void shutdown(Cluster... nodes) {
    shutdown(Arrays.asList(nodes));
  }

  private void shutdown(Collection<Cluster> nodes) {
    for (Cluster node : nodes) {
      try {
        node.shutdown().get();
      } catch (Exception ex) {
        LOGGER.error("Exception on cluster shutdown", ex);
      }
    }
  }
}
