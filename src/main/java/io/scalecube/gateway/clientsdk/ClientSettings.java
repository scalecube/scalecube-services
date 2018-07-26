package io.scalecube.gateway.clientsdk;

public class ClientSettings {

  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_CONTENT_TYPE = "application/json";

  private final String host;
  private final int port;
  private final String contentType;

  private ClientSettings(Builder builder) {
    this.host = builder.host;
    this.port = builder.port;
    this.contentType = builder.contentType;
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public String contentType() {
    return this.contentType;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "ClientSettings{" +
        "host='" + host + '\'' +
        ", port=" + port +
        ", contentType='" + contentType + '\'' +
        '}';
  }

  public static class Builder {
    private String host = DEFAULT_HOST;
    private int port;
    private String contentType = DEFAULT_CONTENT_TYPE;

    private Builder() {}

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder contentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    public ClientSettings build() {
      return new ClientSettings(this);
    }
  }
}
