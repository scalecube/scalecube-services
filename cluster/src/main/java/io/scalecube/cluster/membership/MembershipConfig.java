package io.scalecube.cluster.membership;

import io.scalecube.transport.Address;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MembershipConfig {

  public static final List<Address> DEFAULT_SEED_MEMBERS = Collections.emptyList();
  public static final Map<String, String> DEFAULT_METADATA = Collections.emptyMap();
  public static final int DEFAULT_SYNC_INTERVAL = 30_000;
  public static final int DEFAULT_SYNC_TIMEOUT = 1_000;
  public static final int DEFAULT_SUSPECT_TIMEOUT = 3_000;
  public static final String DEFAULT_SYNC_GROUP = "default";

  private final List<Address> seedMembers;
  private final Map<String, String> metadata;
  private final int syncInterval;
  private final int syncTimeout;
  private final int suspectTimeout;
  private final String syncGroup;

  private MembershipConfig(Builder builder) {
    this.seedMembers = Collections.unmodifiableList(builder.seedMembers);
    this.metadata = Collections.unmodifiableMap(builder.metadata);
    this.syncInterval = builder.syncInterval;
    this.syncTimeout = builder.syncTimeout;
    this.suspectTimeout = builder.suspectTimeout;
    this.syncGroup = builder.syncGroup;
  }

  public static MembershipConfig defaultConfig() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public List<Address> getSeedMembers() {
    return seedMembers;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public int getSyncInterval() {
    return syncInterval;
  }

  public int getSyncTimeout() {
    return syncTimeout;
  }

  public int getSuspectTimeout() {
    return suspectTimeout;
  }

  public String getSyncGroup() {
    return syncGroup;
  }

  @Override
  public String toString() {
    return "MembershipConfigF{seedMembers='" + seedMembers + '\''
        + ", metadata=" + metadata
        + ", syncInterval=" + syncInterval
        + ", syncTimeout=" + syncTimeout
        + ", suspectTimeout=" + suspectTimeout
        + ", syncGroup='" + syncGroup + '\''
        + '}';
  }

  public static final class Builder {

    private List<Address> seedMembers = DEFAULT_SEED_MEMBERS;
    private Map<String, String> metadata = DEFAULT_METADATA;
    private int syncInterval = DEFAULT_SYNC_INTERVAL;
    private int syncTimeout = DEFAULT_SYNC_TIMEOUT;
    private int suspectTimeout = DEFAULT_SUSPECT_TIMEOUT;
    private String syncGroup = DEFAULT_SYNC_GROUP;

    private Builder() {}

    public Builder metadata(Map<String, String> metadata) {
      this.metadata = new HashMap<>(metadata);
      return this;
    }

    public Builder seedMembers(Address... seedMembers) {
      this.seedMembers = Arrays.asList(seedMembers);
      return this;
    }

    public Builder seedMembers(List<Address> seedMembers) {
      this.seedMembers = new ArrayList<>(seedMembers);
      return this;
    }

    public Builder syncInterval(int syncInterval) {
      this.syncInterval = syncInterval;
      return this;
    }

    public Builder syncTimeout(int syncTimeout) {
      this.syncTimeout = syncTimeout;
      return this;
    }

    public Builder suspectTimeout(int suspectTimeout) {
      this.suspectTimeout = suspectTimeout;
      return this;
    }

    public Builder syncGroup(String syncGroup) {
      this.syncGroup = syncGroup;
      return this;
    }

    public MembershipConfig build() {
      return new MembershipConfig(this);
    }
  }
}
