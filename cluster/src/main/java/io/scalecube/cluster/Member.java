package io.scalecube.cluster;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Address;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

/**
 * Cluster member which represents node in the cluster and contains its id, address and metadata.
 *
 * @author Anton Kharenko
 */
@Immutable
public final class Member {

  private final String id;
  private final Address address;
  private final Map<String, String> metadata;

  /**
   * Create instance of cluster member with given id, address and empty metadata.
   *
   * @param id member id
   * @param address address on which given member listens for incoming messages
   */
  public Member(String id, Address address) {
    this(id, address, Collections.emptyMap());
  }

  /**
   * Create instance of cluster member with given parameters.
   *
   * @param id member id
   * @param address address on which given member listens for incoming messages
   * @param metadata member's metadata
   */
  public Member(String id, Address address, Map<String, String> metadata) {
    checkArgument(id != null);
    checkArgument(address != null);
    this.id = id;
    this.address = address;
    this.metadata = metadata != null ? new HashMap<>(metadata) : Collections.emptyMap();
  }

  public String id() {
    return id;
  }

  public Address address() {
    return address;
  }

  public Map<String, String> metadata() {
    return Collections.unmodifiableMap(metadata);
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || getClass() != that.getClass()) {
      return false;
    }
    Member member = (Member) that;
    return Objects.equals(id, member.id)
        && Objects.equals(address, member.address)
        && Objects.equals(metadata, member.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, address, metadata);
  }

  @Override
  public String toString() {
    return id + "@" + address + (metadata.isEmpty() ? "" : metadata);
  }
}
