package io.scalecube.services.methods;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.api.DynamicQualifier;
import io.scalecube.services.api.Qualifier;
import java.lang.reflect.Type;
import java.util.StringJoiner;
import reactor.core.scheduler.Scheduler;

public final class MethodInfo {

  private final String serviceName;
  private final String methodName;
  private final String qualifier;
  private final DynamicQualifier dynamicQualifier;
  private final Type parameterizedReturnType;
  private final boolean isReturnTypeServiceMessage;
  private final CommunicationMode communicationMode;
  private final int parameterCount;
  private final Class<?> requestType;
  private final boolean isRequestTypeServiceMessage;
  private final boolean isSecured;
  private final Scheduler scheduler;

  /**
   * Create a new service info.
   *
   * @param serviceName the name of the service
   * @param methodName the name of the methof
   * @param parameterizedReturnType the return type (with generics support)
   * @param isReturnTypeServiceMessage is return service message
   * @param communicationMode the directions of the method
   * @param parameterCount amount of parameters
   * @param requestType the type of the request
   * @param isRequestTypeServiceMessage is request service message
   * @param isSecured is method protected by authentication
   * @param scheduler scheduler
   */
  public MethodInfo(
      String serviceName,
      String methodName,
      Type parameterizedReturnType,
      boolean isReturnTypeServiceMessage,
      CommunicationMode communicationMode,
      int parameterCount,
      Class<?> requestType,
      boolean isRequestTypeServiceMessage,
      boolean isSecured,
      Scheduler scheduler) {
    this.parameterizedReturnType = parameterizedReturnType;
    this.isReturnTypeServiceMessage = isReturnTypeServiceMessage;
    this.communicationMode = communicationMode;
    this.serviceName = serviceName;
    this.methodName = methodName;
    this.qualifier = Qualifier.asString(serviceName, methodName);
    this.dynamicQualifier = qualifier.contains(":") ? new DynamicQualifier(qualifier) : null;
    this.parameterCount = parameterCount;
    this.requestType = requestType;
    this.isRequestTypeServiceMessage = isRequestTypeServiceMessage;
    this.isSecured = isSecured;
    this.scheduler = scheduler;
  }

  public String serviceName() {
    return serviceName;
  }

  public String methodName() {
    return methodName;
  }

  public String qualifier() {
    return qualifier;
  }

  public DynamicQualifier dynamicQualifier() {
    return dynamicQualifier;
  }

  public Type parameterizedReturnType() {
    return parameterizedReturnType;
  }

  public boolean isReturnTypeServiceMessage() {
    return isReturnTypeServiceMessage;
  }

  public CommunicationMode communicationMode() {
    return communicationMode;
  }

  public int parameterCount() {
    return parameterCount;
  }

  public boolean isRequestTypeServiceMessage() {
    return isRequestTypeServiceMessage;
  }

  public boolean isRequestTypeVoid() {
    return requestType.isAssignableFrom(Void.TYPE);
  }

  public Class<?> requestType() {
    return requestType;
  }

  public boolean isSecured() {
    return isSecured;
  }

  public Scheduler scheduler() {
    return scheduler;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", MethodInfo.class.getSimpleName() + "[", "]")
        .add("serviceName='" + serviceName + "'")
        .add("methodName='" + methodName + "'")
        .add("qualifier='" + qualifier + "'")
        .add("dynamicQualifier=" + dynamicQualifier)
        .add("parameterizedReturnType=" + parameterizedReturnType)
        .add("isReturnTypeServiceMessage=" + isReturnTypeServiceMessage)
        .add("communicationMode=" + communicationMode)
        .add("parameterCount=" + parameterCount)
        .add("requestType=" + requestType)
        .add("isRequestTypeServiceMessage=" + isRequestTypeServiceMessage)
        .add("isSecured=" + isSecured)
        .add("scheduler=" + scheduler)
        .toString();
  }
}
