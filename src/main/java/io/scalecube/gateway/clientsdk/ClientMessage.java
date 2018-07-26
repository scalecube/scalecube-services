package io.scalecube.gateway.clientsdk;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class ClientMessage {

  private Map<String, String> headers;
  private Object data;

  public ClientMessage(Builder builder) {
    this.headers = Collections.unmodifiableMap(new HashMap<>(builder.headers));
    this.data = builder.data;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder from(ClientMessage message) {
    return new Builder(message);
  }

  public String qualifier() {
    return headers.get("q");
  }

  public Map<String, String> headers() {
    return headers;
  }

  public <T> T data() {
    // noinspection unchecked
    return (T) data;
  }

  public boolean hasData() {
    return data != null;
  }

  public boolean hasData(Class<?> dataClass) {
    if (dataClass == null) {
      return false;
    }
    if (dataClass.isPrimitive()) {
      return hasData();
    } else {
      return dataClass.isInstance(data);
    }
  }

  @Override
  public String toString() {
    return "ClientMessage {headers: " + headers + ", data: " + data + '}';
  }

  public static class Builder {
    private Map<String, String> headers = new HashMap<>();
    private Object data;

    private Builder() {}

    public Builder(ClientMessage message) {
      this.headers = message.headers;
      this.data = message.data;
    }

    public Builder header(String key, String value) {
      headers.put(key, value);
      return this;
    }

    public Builder qualifier(String qualifier) {
      return header("q", qualifier);
    }

    public Builder data(Object data) {
      this.data = data;
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      this.headers.putAll(headers);
      return this;
    }

    public ClientMessage build() {
      return new ClientMessage(this);
    }
  }
}
