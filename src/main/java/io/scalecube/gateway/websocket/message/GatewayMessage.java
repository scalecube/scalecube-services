package io.scalecube.gateway.websocket.message;

import io.scalecube.services.api.ServiceMessage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GatewayMessage {

  public static final String QUALIFIER_FIELD = "q";
  public static final String STREAM_ID_FIELD = "sid";
  public static final String SIGNAL_FIELD = "sig";
  public static final String DATA_FIELD = "d";
  public static final String INACTIVITY_FIELD = "i";

  static final String SERVICE_MESSAGE_HEADER_DATA_TYPE = "_type";
  static final String SERVICE_MESSAGE_HEADER_DATA_FORMAT = "_data_format";

  private final String qualifier;
  private final Long streamId;
  private final Integer signal;
  private final Object data;
  private final Integer inactivity;
  private final Map<String, String> headers;

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
    if (msg.headers != null) {
      builder.headers = new HashMap<>(msg.headers);
    }
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
    if (serviceMessage.hasData()) {
      builder.data = serviceMessage.data();
    }
    serviceMessage
        .headers()
        .forEach(
            (key, value) -> {
              switch (key) {
                case QUALIFIER_FIELD:
                  builder.qualifier(value);
                  break;
                case STREAM_ID_FIELD:
                  builder.streamId(Long.parseLong(value));
                  break;
                case SIGNAL_FIELD:
                  builder.signal(Integer.parseInt(value));
                  break;
                case INACTIVITY_FIELD:
                  builder.inactivity(Integer.parseInt(value));
                  break;
                case SERVICE_MESSAGE_HEADER_DATA_FORMAT:
                case SERVICE_MESSAGE_HEADER_DATA_TYPE:
                  break;
                default:
                  builder.header(key, value);
              }
            });
    return builder;
  }

  private GatewayMessage(Builder builder) {
    this.qualifier = builder.qualifier;
    this.streamId = builder.streamId;
    this.signal = builder.signal;
    this.data = builder.data;
    this.inactivity = builder.inactivity;
    this.headers =
        builder.headers != null
            ? Collections.unmodifiableMap(builder.headers)
            : Collections.emptyMap();
  }

  public static GatewayMessage toGatewayMessage(ServiceMessage serviceMessage) {
    return from(serviceMessage).build();
  }

  public ServiceMessage toServiceMessage() {
    return toServiceMessage(this);
  }

  /**
   * {@link GatewayMessage} to {@link ServiceMessage} converter.
   *
   * @param gatewayMessage gateway message
   * @return service message
   */
  public static ServiceMessage toServiceMessage(GatewayMessage gatewayMessage) {
    ServiceMessage.Builder builder =
        ServiceMessage.builder().qualifier(gatewayMessage.qualifier).data(gatewayMessage.data);
    if (gatewayMessage.headers != null && !gatewayMessage.headers.isEmpty()) {
      gatewayMessage.headers.forEach(builder::header);
    }
    if (gatewayMessage.streamId != null) {
      builder.header(STREAM_ID_FIELD, String.valueOf(gatewayMessage.streamId()));
    }
    if (gatewayMessage.signal != null) {
      builder.header(SIGNAL_FIELD, String.valueOf(gatewayMessage.signal()));
    }
    if (gatewayMessage.inactivity != null) {
      builder.header(INACTIVITY_FIELD, String.valueOf(gatewayMessage.inactivity()));
    }
    return builder.build();
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

  public Map<String, String> headers() {
    return headers;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GatewayMessage{");
    sb.append("qualifier='").append(qualifier).append('\'');
    sb.append(", streamId=").append(streamId);
    sb.append(", signal=").append(signal);
    sb.append(", data=").append(data);
    sb.append(", inactivity=").append(inactivity);
    sb.append(", headers=").append(headers);
    sb.append('}');
    return sb.toString();
  }

  public static class Builder {

    private String qualifier;
    private Long streamId;
    private Integer signal;
    private Object data;
    private Integer inactivity;
    private Map<String, String> headers;

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
     * Add a header.
     *
     * @param key header name
     * @param value header value
     * @return self
     */
    public Builder header(String key, String value) {
      if (headers == null) {
        headers = new HashMap<>();
      }
      headers.put(key, value);
      return this;
    }

    public Builder header(String key, Object value) {
      return header(key, value.toString());
    }

    /**
     * Finally build the {@link GatewayMessage} from current builder.
     *
     * @return {@link GatewayMessage} with parameters from current builder.
     */
    public GatewayMessage build() {
      return new GatewayMessage(this);
    }
  }
}
