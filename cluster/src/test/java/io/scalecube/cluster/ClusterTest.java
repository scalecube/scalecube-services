package io.scalecube.cluster;

import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.testlib.BaseTest;

public class ClusterTest extends BaseTest {

  @Test
  public void testJoinDynamicPort() throws Exception {
    // Start seed node
    ICluster seedNode = Cluster.joinAwait();

    // Start other nodes
    int membersNum = 10;
    long start = System.currentTimeMillis();
    List<ICluster> otherNodes = new ArrayList<>();
    for (int i = 0; i < membersNum; i++) {
      otherNodes.add(Cluster.joinAwait(seedNode.address()));
    }
    long end = System.currentTimeMillis();
    System.out.println("Time: " + (end - start) + " millis");
    assertEquals(membersNum + 1, seedNode.members().size());
    System.out.println("Cluster nodes: " + seedNode.members());

    // Shutdown all nodes
    shutdown(seedNode);
    shutdown(otherNodes);
  }

  @Test
  public void testUpdateMetadata() throws Exception {
    // Start seed member
    ICluster seedNode = Cluster.joinAwait();

    // Start member with metadata
    Map<String, String> metadata = ImmutableMap.of("key1", "value1", "key2", "value2");
    ICluster metadataNode = Cluster.joinAwait(metadata, seedNode.address());

    // Start other test members
    int testMembersNum = 10;
    List<ICluster> otherNodes = new ArrayList<>();
    for (int i = 0; i < testMembersNum; i++) {
      otherNodes.add(Cluster.joinAwait(seedNode.address()));
    }

    // Check all test members know valid metadata
    for (ICluster node : otherNodes) {
      Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
      assertTrue(mNodeOpt.isPresent());
      Member mNode = mNodeOpt.get();
      assertEquals(metadata, mNode.metadata());
    }

    // Subscribe for membership update event all nodes
    CountDownLatch updateLatch = new CountDownLatch(testMembersNum);
    for (ICluster node : otherNodes) {
      node.listenMembership()
          .filter(MembershipEvent::isUpdated)
          .subscribe(event -> updateLatch.countDown());
    }


    // Update metadata
    Map<String, String> updatedMetadata = ImmutableMap.of("key1", "value3");
    metadataNode.updateMetadata(updatedMetadata);

    // Await latch
    updateLatch.await(100, TimeUnit.SECONDS);

    // Check all nodes had updated metadata member
    for (ICluster node : otherNodes) {
      Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
      assertTrue(mNodeOpt.isPresent());
      Member mNode = mNodeOpt.get();
      assertEquals(updatedMetadata, mNode.metadata());
    }

    // Shutdown all nodes
    shutdown(seedNode);
    shutdown(metadataNode);
    shutdown(otherNodes);
  }

  @Test
  public void testUpdateMetadataProperty() throws Exception {
    // Start seed member
    ICluster seedNode = Cluster.joinAwait();

    // Start member with metadata
    Map<String, String> metadata = ImmutableMap.of("key1", "value1", "key2", "value2");
    ICluster metadataNode = Cluster.joinAwait(metadata, seedNode.address());

    // Start other test members
    int testMembersNum = 10;
    List<ICluster> otherNodes = new ArrayList<>();
    for (int i = 0; i < testMembersNum; i++) {
      otherNodes.add(Cluster.joinAwait(seedNode.address()));
    }

    // Check all test members know valid metadata
    for (ICluster node : otherNodes) {
      Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
      assertTrue(mNodeOpt.isPresent());
      Member mNode = mNodeOpt.get();
      assertEquals(metadata, mNode.metadata());
    }

    // Subscribe for membership update event all nodes
    CountDownLatch updateLatch = new CountDownLatch(testMembersNum);
    for (ICluster node : otherNodes) {
      node.listenMembership()
          .filter(MembershipEvent::isUpdated)
          .subscribe(event -> updateLatch.countDown());
    }

    // Update metadata
    metadataNode.updateMetadataProperty("key2", "value3");

    // Await latch
    updateLatch.await(100, TimeUnit.SECONDS);

    // Check all nodes had updated metadata member
    for (ICluster node : otherNodes) {
      Optional<Member> mNodeOpt = node.member(metadataNode.member().id());
      assertTrue(mNodeOpt.isPresent());
      Member mNode = mNodeOpt.get();
      Map<String, String> mNodeMetadata = mNode.metadata();
      assertEquals(2, mNode.metadata().size());
      assertEquals("value1", mNodeMetadata.get("key1"));
      assertEquals("value3", mNodeMetadata.get("key2"));
    }

    // Shutdown all nodes
    shutdown(seedNode);
    shutdown(metadataNode);
    shutdown(otherNodes);
  }

  private void shutdown(ICluster... nodes) {
    shutdown(Arrays.asList(nodes));
  }

  private void shutdown(Collection<ICluster> nodes) {
    for (ICluster node : nodes) {
      try {
        node.shutdown().get();
      } catch (Exception ex) {
        LOGGER.error("Exception on cluster shutdown", ex);
      }
    }
  }
}
