package io.scalecube.services.methods;

import static io.scalecube.services.api.DynamicQualifier.isDynamicQualifier;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.api.DynamicQualifier;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.auth.Secured;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import reactor.core.scheduler.Scheduler;

public class MethodInfo {

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
  private final Secured secured;
  private final Scheduler scheduler;
  private final String restMethod;
  private final Collection<String> allowedRoles;
  private final Collection<String> allowedPermissions;

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
   * @param secured secured (optional)
   * @param scheduler scheduler (optional)
   * @param restMethod restMethod (optional)
   * @param serviceRoles serviceRoles (optional)
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
      Secured secured,
      Scheduler scheduler,
      String restMethod,
      Collection<ServiceRoleDefinition> serviceRoles) {
    this.parameterizedReturnType = parameterizedReturnType;
    this.isReturnTypeServiceMessage = isReturnTypeServiceMessage;
    this.communicationMode = communicationMode;
    this.serviceName = serviceName;
    this.methodName = methodName;
    this.qualifier = Qualifier.asString(serviceName, methodName);
    this.dynamicQualifier = isDynamicQualifier(qualifier) ? DynamicQualifier.from(qualifier) : null;
    this.parameterCount = parameterCount;
    this.requestType = requestType;
    this.isRequestTypeServiceMessage = isRequestTypeServiceMessage;
    this.secured = secured;
    this.scheduler = scheduler;
    this.restMethod = restMethod;
    this.allowedRoles =
        serviceRoles.stream().map(ServiceRoleDefinition::role).collect(Collectors.toSet());
    this.allowedPermissions =
        serviceRoles.stream()
            .flatMap(definition -> definition.permissions().stream())
            .collect(Collectors.toSet());
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

  public Secured secured() {
    return secured;
  }

  public Scheduler scheduler() {
    return scheduler;
  }

  public String restMethod() {
    return restMethod;
  }

  public Collection<String> allowedRoles() {
    return allowedRoles;
  }

  public Collection<String> allowedPermissions() {
    return allowedPermissions;
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
        .add("secured=" + secured)
        .add("scheduler=" + scheduler)
        .add("restMethod=" + restMethod)
        .add("allowedRoles=" + allowedRoles)
        .add("allowedPermissions=" + allowedPermissions)
        .toString();
  }
}
