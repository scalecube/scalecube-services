package io.scalecube.cluster;

import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.NetworkEmulator;

import rx.Observable;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nonnull;

/**
 * Facade cluster interface which provides API to interact with cluster members.
 * 
 * @author Anton Kharenko
 */
public interface ICluster {

  /**
   * Returns local listen {@link Address} of this cluster instance.
   */
  Address address();

  void send(Member member, Message message);

  void send(Member member, Message message, CompletableFuture<Void> promise);

  void send(Address address, Message message);

  void send(Address address, Message message, CompletableFuture<Void> promise);

  Observable<Message> listen();

  /**
   * Spreads given message between cluster members using gossiping protocol.
   */
  void spreadGossip(Message message);

  /**
   * Listens for gossips from other cluster members.
   */
  Observable<Message> listenGossips();

  /**
   * Returns local cluster member which corresponds to this cluster instance.
   */
  Member member();

  /**
   * Returns cluster member with given id or null if no member with such id exists at joined cluster.
   */
  Optional<Member> member(String id);

  /**
   * Returns cluster member by given address or null if no member with such address exists at joined cluster.
   */
  Optional<Member> member(Address address);

  /**
   * Returns list of all members of the joined cluster. This will include all cluster members including local member.
   */
  Collection<Member> members();

  /**
   * Returns list of all cluster members of the joined cluster excluding local member.
   */
  Collection<Member> otherMembers();

  /**
   * Updates local member metadata with the given metadata map. Metadata is updated asynchronously and results in a
   * membership update event for local member once it is updated locally. Information about new metadata is disseminated
   * to other nodes of the cluster with a weekly-consistent guarantees.
   *
   * @param metadata new metadata
   */
  void updateMetadata(Map<String, String> metadata);

  /**
   * Updates single key-value pair of local member's metadata. This is a shortcut method and anyway update will result
   * in a full metadata update. In case if you need to update several metadata property together it is recommended to
   * use {@link #updateMetadata(Map)}.
   *
   * @param key metadata key to update
   * @param value metadata value to update
   */
  void updateMetadataProperty(String key, String value);

  /**
   * Listen changes in cluster membership.
   */
  Observable<MembershipEvent> listenMembership();

  /**
   * Member notifies other members of the cluster about leaving and gracefully shutdown and free occupied resources.
   *
   * @return Listenable future which is completed once graceful shutdown is finished.
   */
  CompletableFuture<Void> shutdown();

  /**
   * Returns network emulator associated with this instance of cluster. It always returns non null instance even if
   * network emulator is disabled by transport config. In case when network emulator is disable all calls to network
   * emulator instance will result in no operation.
   */
  @Nonnull
  NetworkEmulator networkEmulator();

}
