package io.scalecube.services;

import io.scalecube.services.annotations.ServiceMethod;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ServiceScanner {

  private ServiceScanner() {
    // Do not instantiate
  }

  public static List<ServiceRegistration> toServiceRegistrations(ServiceInfo serviceInfo) {
    return Reflect.serviceInterfaces(serviceInfo.serviceInstance())
        .map(
            serviceInterface -> {
              final var serviceInfoTags = serviceInfo.tags();
              final var apiTags = Reflect.serviceTags(serviceInterface);
              final var tags = new HashMap<>(apiTags);
              // service tags override tags from @Service
              tags.putAll(serviceInfoTags);

              final var namespace = Reflect.serviceName(serviceInterface);
              final var actions =
                  Arrays.stream(serviceInterface.getMethods())
                      .filter(method -> method.isAnnotationPresent(ServiceMethod.class))
                      .map(ServiceMethodDefinition::fromMethod)
                      .toList();

              return new ServiceRegistration(namespace, tags, actions);
            })
        .collect(Collectors.toList());
  }

  public static List<ServiceRegistration> processServiceRegistrations(
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
                                new ServiceMethodDefinition(
                                    replacePlaceholders(methodDefinition.action(), microservices),
                                    methodDefinition.tags(),
                                    methodDefinition.isSecured(),
                                    methodDefinition.restMethod()))
                        .toList()))
        .toList();
  }

  public static String replacePlaceholders(String input, Microservices microservices) {
    final var pattern = Pattern.compile("\\$\\{(.*?)}");
    final var matcher = pattern.matcher(input);
    final var result = new StringBuilder();

    while (matcher.find()) {
      final var key = matcher.group(1);
      final var replacement = parsePlaceholderValue(input, key, microservices);
      matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(result);

    return result.toString();
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
}
