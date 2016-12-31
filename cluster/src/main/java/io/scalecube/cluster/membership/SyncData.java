package io.scalecube.cluster.membership;

import io.protostuff.Tag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.Immutable;

/**
 * A class containing full membership table from specific member and used full synchronization between cluster members.
 *
 * @author Anton Kharenko
 */
@Immutable
final class SyncData {

  /**
   * Full cluster membership table.
   */
  @Tag(1)
  private final List<MembershipRecord> membership;

  /**
   * Sort of cluster identifier. Only members in the same sync group allowed to join into cluster.
   */
  @Tag(2)
  private final String syncGroup;

  public SyncData(Collection<MembershipRecord> membership, String syncGroup) {
    this.membership = new ArrayList<>(membership);
    this.syncGroup = syncGroup;
  }

  public Collection<MembershipRecord> getMembership() {
    return new ArrayList<>(membership);
  }

  public String getSyncGroup() {
    return syncGroup;
  }

  @Override
  public String toString() {
    return "SyncData{membership=" + membership + ", syncGroup=" + syncGroup + '}';
  }

}
