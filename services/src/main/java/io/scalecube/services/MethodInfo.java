package io.scalecube.services;

public class MethodInfo {

  private final Class<?> parameterizedReturnType;
  private final CommunicationMode communicationMode;
  private final boolean isRequestTypeServiceMessage;

  public MethodInfo(Class<?> parameterizedReturnType,
      CommunicationMode communicationMode,
      boolean isRequestTypeServiceMessage) {
    this.parameterizedReturnType = parameterizedReturnType;
    this.communicationMode = communicationMode;
    this.isRequestTypeServiceMessage = isRequestTypeServiceMessage;
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
}
