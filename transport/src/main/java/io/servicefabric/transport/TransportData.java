package io.servicefabric.transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.servicefabric.transport.TransportData.Status.NEW;
import static io.servicefabric.transport.TransportData.Status.RESOLVED_ERR;
import static io.servicefabric.transport.TransportData.Status.RESOLVED_OK;

import io.servicefabric.transport.utils.KvPair;

import io.protostuff.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** DTO class. Used for transport handshake mechanism. */
public class TransportData {
  public static final String Q_TRANSPORT_HANDSHAKE_SYNC = "pt.openapi.core/transport/handshakeSync";
  public static final String Q_TRANSPORT_HANDSHAKE_SYNC_ACK = "pt.openapi.core/transport/handshakeSyncAck";

  public static final String META_ORIGIN_ENDPOINT = "originEndpoint";
  public static final String META_ORIGIN_ENDPOINT_ID = "originEndpointId";

  enum Status {
    /** Initial status. Means handshake just created. */
    NEW,
    /** Handshake passed. Resolution is OK. */
    RESOLVED_OK,
    /** General handshake failure. Resolution is not OK. */
    RESOLVED_ERR
  }

  static class Builder {
    private final TransportData target = new TransportData();

    private Builder(Status status) {
      target.status = status;
    }

    Builder setExplain(String explain) {
      target.explain = explain;
      return this;
    }

    TransportData build() {
      return target;
    }
  }

  /**
   * Metadata about the connection. If status set to RESOLVED_OK this map shall contain connection metadata from remote
   * peer (i.e. not local one). Otherwise -- local metadata shall return unchanged.
   */
  @Tag(1)
  private List<KvPair<String, Object>> metadata = new ArrayList<>();
  /**
   * Status of resolution. When set to RESOLVED_OK this should mean connection is good and we can proceed further with
   * transport. Otherwise -- transport should be treated as invalid and purged from system.
   */
  @Tag(2)
  private Status status;
  /** String explanation of the status. Not set if RESOLVED_OK (but set otherwise). */
  @Tag(3)
  private String explain;

  private TransportData() {}

  static Builder newData(Map<String, Object> metadata) {
    Builder builder = new Builder(NEW);
    populate(builder, metadata);
    return builder;
  }

  static Builder ok(Map<String, Object> metadata) {
    Builder builder = new Builder(RESOLVED_OK);
    populate(builder, metadata);
    return builder;
  }

  static Builder err(TransportData remote) {
    return err(remote, RESOLVED_ERR);
  }

  static Builder err(TransportData remote, Status status) {
    checkArgument(status.ordinal() >= RESOLVED_ERR.ordinal());
    Builder builder = new Builder(status);
    builder.target.metadata = remote.metadata;
    return builder;
  }

  @Nonnull
  Status status() {
    return status;
  }

  @Nullable
  String explain() {
    return explain;
  }

  boolean isResolvedOk() {
    return status() == RESOLVED_OK;
  }

  @Nullable
  <T> T get(String key) {
    for (KvPair<String, Object> pair : metadata) {
      if (pair.getKey().equals(key)) {
        return (T) pair.getValue();
      }
    }
    return null;
  }

  private static void populate(Builder builder, Map<String, Object> metadata) {
    checkArgument(metadata != null, "metadata must not be null");
    checkArgument(!metadata.isEmpty(), "metadata must not be empty");
    for (Map.Entry<String, Object> entry : metadata.entrySet()) {
      String key = entry.getKey();
      checkArgument(!isNullOrEmpty(key), "key must not be null or empty");
      Object value = entry.getValue();
      checkArgument(value != null, "value must not be null, key=" + key);
      builder.target.metadata.add(new KvPair<>(key, value));
    }
  }
}
