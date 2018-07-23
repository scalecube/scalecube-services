package io.scalecube.gateway.websocket.message;

import io.scalecube.services.api.ServiceMessage;

import java.util.Objects;

public class GatewayMessage {

  public static final String QUALIFIER_FIELD = "q";
  public static final String STREAM_ID_FIELD = "sid";
  public static final String SIGNAL_FIELD = "sig";
  public static final String DATA_FIELD = "d";
  public static final String INACTIVITY_FIELD = "i";

  private String qualifier;
  private Long streamId;
  private Integer signal;
  private Object data;
  private Integer inactivity;

  /**
   * Get a builder by pattern form given {@link GatewayMessage}.
   *
   * @param msg Message form where to copy field values.
   * @return builder with fields copied from given {@link GatewayMessage}
   */
  public static Builder from(GatewayMessage msg) {
    Builder builder = new Builder();
    builder.qualifier = msg.qualifier();
    builder.streamId = msg.streamId();
    builder.signal = msg.signal();
    builder.inactivity = msg.inactivity();
    builder.data = msg.data();
    return builder;
  }

  /**
   * Get a builder by pattern form given {@link ServiceMessage}.
   *
   * @param serviceMessage ServiceMessage form where to copy field values.
   * @return builder with fields copied from given {@link ServiceMessage}
   */
  public static Builder from(ServiceMessage serviceMessage) {
    Builder builder = new Builder();
    builder.qualifier = serviceMessage.qualifier();
    if (serviceMessage.hasData()) {
      builder.data = serviceMessage.data();
    }
    if (serviceMessage.header(STREAM_ID_FIELD) != null) {
      builder.streamId = Long.parseLong(serviceMessage.header(STREAM_ID_FIELD));
    }
    if (serviceMessage.header(SIGNAL_FIELD) != null) {
      builder.signal = Integer.parseInt(serviceMessage.header(SIGNAL_FIELD));
    }
    if (serviceMessage.header(INACTIVITY_FIELD) != null) {
      builder.inactivity = Integer.parseInt(serviceMessage.header(INACTIVITY_FIELD));
    }
    return builder;
  }

  public static ServiceMessage toServiceMessage(GatewayMessage gatewayMessage) {
    ServiceMessage.Builder builder = ServiceMessage.builder()
        .qualifier(gatewayMessage.qualifier())
        .data(gatewayMessage.data());
    if (gatewayMessage.streamId() != null) {
      builder.header(STREAM_ID_FIELD, String.valueOf(gatewayMessage.streamId()));
    }
    if (gatewayMessage.signal() != null) {
      builder.header(SIGNAL_FIELD, String.valueOf(gatewayMessage.signal()));
    }
    if (gatewayMessage.inactivity() != null) {
      builder.header(INACTIVITY_FIELD, String.valueOf(gatewayMessage.inactivity()));
    }
    return builder.build();
  }

  public static GatewayMessage toGatewayMessage(ServiceMessage serviceMessage) {
    return from(serviceMessage).build();
  }

  GatewayMessage() {}

  private GatewayMessage(String qualifier, Long streamId, Integer signal, Object data, Integer inactivity) {
    this.qualifier = qualifier;
    this.streamId = streamId;
    this.signal = signal;
    this.data = data;
    this.inactivity = inactivity;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String qualifier() {
    return qualifier;
  }

  public Long streamId() {
    return streamId;
  }

  public Integer signal() {
    return signal;
  }

  public <T> T data() {
    // noinspection unchecked
    return (T) data;
  }

  public Integer inactivity() {
    return inactivity;
  }

  public boolean hasSignal(Signal signal) {
    return this.signal != null && this.signal == signal.code();
  }

  public ServiceMessage toServiceMessage() {
    return toServiceMessage(this);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GatewayMessage{");
    sb.append("qualifier='").append(qualifier).append('\'');
    sb.append(", streamId=").append(streamId);
    sb.append(", signal=").append(signal);
    sb.append(", data=").append(data);
    sb.append(", inactivity=").append(inactivity);
    sb.append('}');
    return sb.toString();
  }

  public static class Builder {

    private String qualifier;
    private Long streamId;
    private Integer signal;
    private Object data;
    private Integer inactivity;

    Builder() {}

    public Builder qualifier(String qualifier) {
      this.qualifier = qualifier;
      return this;
    }

    public Builder streamId(Long streamId) {
      this.streamId = streamId;
      return this;
    }

    public Builder signal(Integer signal) {
      this.signal = signal;
      return this;
    }

    public Builder signal(Signal signal) {
      this.signal = signal.code();
      return this;
    }

    public Builder inactivity(Integer inactivity) {
      this.inactivity = inactivity;
      return this;
    }

    public Builder data(Object data) {
      this.data = Objects.requireNonNull(data);
      return this;
    }

    /**
     * Finally build the {@link GatewayMessage} from current builder.
     *
     * @return {@link GatewayMessage} with parameters from current builder.
     */
    public GatewayMessage build() {
      return new GatewayMessage(qualifier, streamId, signal, data, inactivity);
    }
  }
}
