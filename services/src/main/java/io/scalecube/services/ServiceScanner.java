package io.scalecube.services;

import io.scalecube.services.methods.ServiceMethodDefinition;
import io.scalecube.services.methods.ServiceRoleDefinition;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ServiceScanner {

  private ServiceScanner() {
    // Do not instantiate
  }

  public static List<ServiceRegistration> toServiceRegistrations(ServiceInfo serviceInfo) {
    final var serviceInstance = serviceInfo.serviceInstance();
    return Reflect.serviceInterfaces(serviceInstance)
        .map(
            serviceInterface -> {
              final var serviceInfoTags = serviceInfo.tags();
              final var serviceTags = new HashMap<>(Reflect.serviceTags(serviceInterface));
              // service tags override tags from @Service
              serviceTags.putAll(serviceInfoTags);

              final var serviceInstanceClass = serviceInstance.getClass();
              final var namespace = Reflect.serviceName(serviceInterface);

              final var methodDefinitions =
                  Reflect.serviceMethods(serviceInterface).stream()
                      .map(
                          method -> {
                            // validate method
                            Reflect.validateMethodOrThrow(method);

                            // get service instance method
                            Method serviceMethod;
                            try {
                              serviceMethod =
                                  serviceInstanceClass.getMethod(
                                      method.getName(), method.getParameterTypes());
                            } catch (NoSuchMethodException e) {
                              throw new RuntimeException(e);
                            }

                            return ServiceMethodDefinition.builder()
                                .action(Reflect.methodName(method))
                                .tags(Reflect.serviceMethodTags(method))
                                .restMethod(Reflect.restMethod(method))
                                .secured(Reflect.secured(serviceMethod) != null)
                                .allowedRoles(Reflect.allowedRoles(serviceMethod))
                                .build();
                          })
                      .toList();

              return new ServiceRegistration(namespace, serviceTags, methodDefinitions);
            })
        .collect(Collectors.toList());
  }

  public static List<ServiceRegistration> replacePlaceholders(
      Collection<ServiceRegistration> serviceRegistrations, Microservices microservices) {
    return serviceRegistrations.stream()
        .map(
            registration ->
                new ServiceRegistration(
                    registration.namespace(),
                    registration.tags(),
                    registration.methods().stream()
                        .map(
                            methodDefinition ->
                                ServiceMethodDefinition.builder()
                                    .action(
                                        replacePlaceholders(
                                            methodDefinition.action(), microservices))
                                    .tags(methodDefinition.tags())
                                    .restMethod(methodDefinition.restMethod())
                                    .secured(methodDefinition.isSecured())
                                    .allowedRoles(methodDefinition.allowedRoles())
                                    .build())
                        .toList()))
        .toList();
  }

  public static String replacePlaceholders(String action, Microservices microservices) {
    final var pattern = Pattern.compile("\\$\\{(.*?)}");
    final var matcher = pattern.matcher(action);
    final var result = new StringBuilder();

    while (matcher.find()) {
      final var key = matcher.group(1);
      final var replacement = parsePlaceholderValue(action, key, microservices);
      matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(result);

    return result.toString();
  }

  public static Collection<ServiceRoleDefinition> collectServiceRoles(
      List<Object> serviceInstances) {
    final var collectorMap = new HashMap<String, ServiceRoleDefinition>();
    serviceInstances.forEach(
        serviceInstance -> {
          final var serviceInstanceClass = serviceInstance.getClass();
          Reflect.serviceInterfaces(serviceInstance)
              .forEach(
                  serviceInterface ->
                      Reflect.serviceMethods(serviceInterface)
                          .forEach(
                              method -> {
                                // validate method
                                Reflect.validateMethodOrThrow(method);

                                // get service instance method
                                Method serviceMethod;
                                try {
                                  serviceMethod =
                                      serviceInstanceClass.getMethod(
                                          method.getName(), method.getParameterTypes());
                                } catch (NoSuchMethodException e) {
                                  throw new RuntimeException(e);
                                }

                                Reflect.serviceRoles(serviceMethod)
                                    .forEach(role -> collectServiceRole(role, collectorMap));
                              }));
        });
    return collectorMap.values();
  }

  private static String parsePlaceholderValue(
      String input, String key, Microservices microservices) {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Wrong placeholder qualifier: " + input);
    }

    final var split = key.split(":");
    if (split.length != 2) {
      throw new IllegalArgumentException("Wrong placeholder qualifier: " + input);
    }

    final var lookup = split[0];
    final var lookupVar = split[1];

    if (lookup.equals("microservices")) {
      try {
        return (String) Microservices.class.getMethod(lookupVar).invoke(microservices);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    throw new IllegalArgumentException("Wrong placeholder qualifier: " + input);
  }

  private static void collectServiceRole(
      ServiceRoleDefinition roleDefinition, Map<String, ServiceRoleDefinition> collectorMap) {
    collectorMap.compute(
        roleDefinition.role(),
        (key, value) -> {
          if (value == null) {
            return roleDefinition;
          } else {
            final var permissions = new HashSet<String>();
            permissions.addAll(value.permissions());
            permissions.addAll(roleDefinition.permissions());
            return new ServiceRoleDefinition(key, permissions);
          }
        });
  }
}
