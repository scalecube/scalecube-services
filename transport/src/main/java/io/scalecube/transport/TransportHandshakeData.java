package io.scalecube.transport;

import static io.scalecube.transport.TransportHandshakeData.Status.*;

import io.protostuff.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** DTO class. Used for transport handshake mechanism. */
@Immutable
final class TransportHandshakeData {

  public static final String Q_TRANSPORT_HANDSHAKE_SYNC = "io.scalecube/transport/handshake";
  public static final String Q_TRANSPORT_HANDSHAKE_SYNC_ACK = "io.scalecube/transport/handshakeAck";

  public enum Status {
    /** Initial status. Means handshake just created. */
    CREATED,
    /** Handshake passed. Resolution is OK. */
    RESOLVED_OK,
    /** General handshake failure. Resolution is not OK. */
    RESOLVED_ERROR
  }

  /** Encoded transport endpoint {@code host:port:id} */
  @Tag(1)
  private final String encodedEndpoint;
  /** A {@link Status} field */
  @Tag(3)
  private final Status status;
  /** Human redable explanation */
  @Tag(4)
  private final String explain;
  /** Decoded transport endpoint. */
  private transient TransportEndpoint endpoint;

  private TransportHandshakeData(String encodedEndpoint, Status status, String explain) {
    this.encodedEndpoint = encodedEndpoint;
    this.status = status;
    this.explain = explain;
    this.endpoint = TransportEndpoint.from(encodedEndpoint);
  }

  /** Creates new instance with status {@link Status#CREATED} and given endpoint. */
  public static TransportHandshakeData create(TransportEndpoint endpoint) {
    return new TransportHandshakeData(endpoint.getString(), CREATED, null);
  }

  /** Creates new instance with status {@link Status#RESOLVED_OK} and given endpoint. */
  public static TransportHandshakeData ok(TransportEndpoint endpoint) {
    return new TransportHandshakeData(endpoint.getString(), RESOLVED_OK, null);
  }

  /** Creates new instance with status {@link Status#RESOLVED_ERROR} and given error message explanation. */
  public static TransportHandshakeData error(TransportEndpoint endpoint, String explain) {
    return new TransportHandshakeData(endpoint.getString(), RESOLVED_ERROR, explain);
  }

  /** Transport endpoint related to the opposite end of the corresponding connection; never null. */
  @Nonnull
  public TransportEndpoint getEndpoint() {
    return endpoint != null ? endpoint : (endpoint = TransportEndpoint.from(encodedEndpoint));
  }

  /**
   * Status of resolution. When set to {@link Status#RESOLVED_OK} this should mean connection is good and we can proceed
   * further with transport. Otherwise -- transport should be treated as invalid and purged from system.
   */
  @Nonnull
  public Status getStatus() {
    return status;
  }

  /** String explanation of the status. Not set if {@link Status#RESOLVED_OK} (but set otherwise). */
  @Nullable
  public String getExplain() {
    return explain;
  }

  /** Returns true if status is {@link Status#RESOLVED_OK}; false otherwise. */
  boolean isResolvedOk() {
    return getStatus() == RESOLVED_OK;
  }

  @Override
  public String toString() {
    return "TransportHandshakeData{"
        + "endpoint=" + getEndpoint()
        + ", status=" + getStatus()
        + ", explain='" + explain + '\''
        + '}';
  }
}
