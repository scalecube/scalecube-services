package io.scalecube.services;

import io.scalecube.services.annotations.Service;
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
   * Scans {@code ServiceInfo} and builds list of {@code ServiceRegistration}-s.
   *
   * @param serviceInfo service info instance
   * @return list of {@code ServiceRegistration}-s
   */
  public static List<ServiceRegistration> scanServiceInfo(ServiceInfo serviceInfo) {
    return Arrays.stream(serviceInfo.serviceInstance().getClass().getInterfaces())
        .filter(serviceInterface -> serviceInterface.isAnnotationPresent(Service.class))
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
                      .map(
                          method ->
                              new ServiceMethodDefinition(
                                  Reflect.methodName(method), Reflect.serviceMethodTags(method)))
                      .collect(Collectors.toList());
              return new ServiceRegistration(namespace, serviceTags, actions);
            })
        .collect(Collectors.toList());
  }

  /** Tuple class. Contains service interface along with tags map. */
  private static class InterfaceInfo {
    private final Class<?> serviceInterface;
    private final Map<String, String> tags;

    private InterfaceInfo(Class<?> serviceInterface, Map<String, String> tags) {
      this.serviceInterface = serviceInterface;
      this.tags = tags;
    }
  }
}
