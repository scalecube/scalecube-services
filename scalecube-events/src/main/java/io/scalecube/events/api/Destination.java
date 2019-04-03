package io.scalecube.events.api;

public class Destination {
  
  String host;
  int port;
  int controlPort;

  public Destination(String host, int port, int controlPort) {
    this.host = host;
    this.port = port;
    this.controlPort = controlPort;
  }

  public String host() {
    return this.host;
  }

  public int port() {
    return this.port;
  }

  public int controlPort() {
    return this.controlPort;
  }
  
  @Override
  public String toString() {
    return "Destination [host=" + host + ", port=" + port + ", controlPort=" + controlPort + "]";
  }
}
