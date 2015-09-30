package io.servicefabric.transport;

import static io.servicefabric.transport.TransportHandshakeData.Status.CREATED;
import static io.servicefabric.transport.TransportHandshakeData.Status.RESOLVED_ERROR;
import static io.servicefabric.transport.TransportHandshakeData.Status.RESOLVED_OK;

import io.protostuff.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** DTO class. Used for transport handshake mechanism. */
@Immutable
final class TransportHandshakeData {

  public static final String Q_TRANSPORT_HANDSHAKE_SYNC = "io.servicefabric/transport/handshake";
  public static final String Q_TRANSPORT_HANDSHAKE_SYNC_ACK = "io.servicefabric/transport/handshakeAck";

  public enum Status {
    /** Initial status. Means handshake just created. */
    CREATED,

    /** Handshake passed. Resolution is OK. */
    RESOLVED_OK,

    /** General handshake failure. Resolution is not OK. */
    RESOLVED_ERROR
  }

  @Tag(1)
  private final TransportEndpoint endpoint;
  @Tag(2)
  private final Status status;
  @Tag(3)
  private final String explain;

  private TransportHandshakeData(TransportEndpoint endpoint, Status status, String explain) {
    this.endpoint = endpoint;
    this.status = status;
    this.explain = explain;
  }

  /** Creates new instance with status CREATED and given endpoint. */
  public static TransportHandshakeData create(TransportEndpoint endpoint) {
    return new TransportHandshakeData(endpoint, CREATED, null);
  }

  /** Creates new instance with status RESOLVED_OK and given endpoint. */
  public static TransportHandshakeData ok(TransportEndpoint endpoint) {
    return new TransportHandshakeData(endpoint, RESOLVED_OK, null);
  }

  /** Creates new instance with status RESOLVED_ERROR and given error message explanation. */
  public static TransportHandshakeData error(String explain) {
    return new TransportHandshakeData(null, RESOLVED_ERROR, explain);
  }

  /**
   * Transport endpoint related to the opposite end of the corresponding connection. In case of error it will be null.
   */
  @Nullable
  public TransportEndpoint endpoint() {
    return endpoint;
  }

  /**
   * Status of resolution. When set to RESOLVED_OK this should mean connection is good and we can proceed further with
   * transport. Otherwise -- transport should be treated as invalid and purged from system.
   */
  @Nonnull
  public Status status() {
    return status;
  }

  /** String explanation of the status. Not set if RESOLVED_OK (but set otherwise). */
  @Nullable
  public String explain() {
    return explain;
  }

  /** Returns true if status is RESOLVED_OK; false otherwise. */
  boolean isResolvedOk() {
    return status() == RESOLVED_OK;
  }

  @Override
  public String toString() {
    return "TransportHandshakeData{"
        + "endpoint=" + endpoint
        + ", status=" + status
        + ", explain='" + explain + '\''
        + '}';
  }

}
