package io.scalecube.services;

import io.scalecube.services.annotations.ServiceMethod;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ServiceScanner {

  private ServiceScanner() {
    // Do not instantiate
  }

  /**
   * Scans {@code ServiceInfo} and builds list of {@code ServiceRegistration} objects.
   *
   * @param serviceInfo service info instance
   * @return list of {@code ServiceRegistration} objects
   */
  public static List<ServiceRegistration> scanServiceInfo(ServiceInfo serviceInfo) {
    return Reflect.serviceInterfaces(serviceInfo.serviceInstance())
        .map(
            serviceInterface -> {
              Map<String, String> serviceInfoTags = serviceInfo.tags();
              Map<String, String> apiTags = Reflect.serviceTags(serviceInterface);
              Map<String, String> buffer = new HashMap<>(apiTags);
              // service tags override tags from @Service
              buffer.putAll(serviceInfoTags);
              return new InterfaceInfo(serviceInterface, Collections.unmodifiableMap(buffer));
            })
        .map(
            interfaceInfo -> {
              Class<?> serviceInterface = interfaceInfo.serviceInterface;
              Map<String, String> serviceTags = interfaceInfo.tags;
              String namespace = Reflect.serviceName(serviceInterface);
              List<ServiceMethodDefinition> actions =
                  Arrays.stream(serviceInterface.getMethods())
                      .filter(method -> method.isAnnotationPresent(ServiceMethod.class))
                      .map(ServiceMethodDefinition::fromMethod)
                      .collect(Collectors.toList());
              return new ServiceRegistration(namespace, serviceTags, actions);
            })
        .collect(Collectors.toList());
  }

  private record InterfaceInfo(Class<?> serviceInterface, Map<String, String> tags) {}
}
