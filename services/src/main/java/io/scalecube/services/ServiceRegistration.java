package io.scalecube.services;

public class ServiceRegistration {

  private final String memberId;
  private final String serviceName; 
  private final String ip; 
  private final int port;
  
  public String memberId() {
    return memberId;
  }
  public String serviceName() {
    return serviceName;
  }
  public String ip() {
    return ip;
  }
  public int port() {
    return port;
  }
  
  private ServiceRegistration(String memberId, String serviceName, String ip, int port){
    this.memberId = memberId;
    this.serviceName = serviceName;
    this.ip = ip;
    this.port = port;
  }
  public static ServiceRegistration create(String memberId, String serviceName, String ip, int port){
    return new ServiceRegistration(memberId, serviceName, ip, port);
  }
  
  
}
