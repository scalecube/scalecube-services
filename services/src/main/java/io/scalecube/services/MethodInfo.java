package io.scalecube.services;

public class MethodInfo {

  private final String serviceName;
  private final Class<?> parameterizedReturnType;
  private final CommunicationMode communicationMode;
  private final boolean isRequestTypeServiceMessage;
  private final String name;
  private final int parameterCount;

  public MethodInfo(String serviceName,
      Class<?> parameterizedReturnType,
      CommunicationMode communicationMode,
      boolean isRequestTypeServiceMessage,
      String name,
      int parameterCount) {
    
    this.parameterizedReturnType = parameterizedReturnType;
    this.communicationMode = communicationMode;
    this.isRequestTypeServiceMessage = isRequestTypeServiceMessage;
    this.serviceName = serviceName;
    this.name = name;
    this.parameterCount = parameterCount;
  }

  public String serviceName() {
    return serviceName;
  }

  public Class<?> parameterizedReturnType() {
    return parameterizedReturnType;
  }

  public CommunicationMode communicationMode() {
    return communicationMode;
  }

  public boolean isRequestTypeServiceMessage() {
    return isRequestTypeServiceMessage;
  }

  public String methodName() {
    return name;
  }

  public int parameterCount() {
    return parameterCount;
  }
}
