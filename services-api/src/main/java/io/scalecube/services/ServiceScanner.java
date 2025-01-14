package io.scalecube.services;

import io.scalecube.services.annotations.ServiceMethod;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
}
