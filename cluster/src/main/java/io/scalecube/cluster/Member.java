package io.scalecube.cluster;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.scalecube.transport.Address;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author Anton Kharenko
 */
public class Member {

  private final String id;
  private final Address address;
  private final Map<String, String> metadata;

  public Member(String id, Address address, Map<String, String> metadata) {
    checkArgument(id != null);
    checkArgument(address != null);
    this.id = id;
    this.address = address;
    this.metadata = metadata != null ? new HashMap<>(metadata) : Collections.<String, String>emptyMap();
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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Member member = (Member) o;
    return Objects.equals(id, member.id) &&
        Objects.equals(address, member.address) &&
        Objects.equals(metadata, member.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, address, metadata);
  }

  @Override
  public String toString() {
    return "Member{" +
        "id='" + id + '\'' +
        ", address=" + address +
        ", metadata=" + metadata +
        '}';
  }
}
