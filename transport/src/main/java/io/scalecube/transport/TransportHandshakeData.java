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

  /**
   * Encoded transport endpoint {@code host:port:id}. <b>NOTE:</b> {@code host} isn't optinal here. This is DTO field
   * purely for populating {@link #endpoint} property. By itself this is transport endpoint related to the opposite end
   * of the corresponding connection; never null.
   */
  @Tag(1)
  private final String encodedEndpoint;

  /**
   * A status field. When set to {@link Status#RESOLVED_OK} this mean transport connection is good and we can proceed
   * further with transport, otherwise -- transport should be treated as invalid and purged from system; never null.
   */
  @Tag(2)
  private final Status status;

  /**
   * Human redable explanation of status field; never null.
   */
  @Tag(3)
  private final String explain;

  /**
   * Decoded transport endpoint. <b>NOTE:</b> this is calculated field from {@link #encodedEndpoint} property. By itself
   * this is transport endpoint related to the opposite end of the corresponding connection; never null.
   */
  private transient volatile TransportEndpoint endpoint;

  private TransportHandshakeData(String encodedEndpoint, Status status, String explain) {
    this.encodedEndpoint = encodedEndpoint;
    this.status = status;
    this.explain = explain;
    this.endpoint = TransportEndpoint.from(encodedEndpoint);
  }

  static TransportHandshakeData create(TransportEndpoint endpoint) {
    return new TransportHandshakeData(endpoint.getString(), CREATED, CREATED.toString());
  }

  static TransportHandshakeData ok(TransportEndpoint endpoint) {
    return new TransportHandshakeData(endpoint.getString(), RESOLVED_OK, RESOLVED_OK.toString());
  }

  static TransportHandshakeData error(TransportEndpoint endpoint, String explain) {
    return new TransportHandshakeData(endpoint.getString(), RESOLVED_ERROR, explain);
  }

  /** See {@link #endpoint} */
  @Nonnull
  TransportEndpoint getEndpoint() {
    return endpoint != null ? endpoint : (endpoint = TransportEndpoint.from(encodedEndpoint));
  }

  /** See {@link #status} */
  @Nonnull
  Status getStatus() {
    return status;
  }

  /** String explanation of the status. Not set if {@link Status#RESOLVED_OK} (but set otherwise). */
  @Nullable
  String getExplain() {
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
