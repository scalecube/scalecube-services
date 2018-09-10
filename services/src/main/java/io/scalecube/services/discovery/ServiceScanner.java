package io.scalecube.services.discovery;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceMethodDefinition;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.codec.DataCodec;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ServiceScanner {

  private ServiceScanner() {
    // Do not instantiate
  }

  /**
   * Scans all passed services instances along with service address information and builds a {@link
   * ServiceEndpoint} object.
   *
   * @param serviceInstances services instances collection
   * @param endpointId endpoint string identifier
   * @param host endpoint service host
   * @param port endpoint service port
   * @param endpointTags map of tags defined at endpoint level
   * @return newly created instance of {@link ServiceEndpoint} object
   */
  public static ServiceEndpoint scan(
      List<ServiceInfo> serviceInstances,
      String endpointId,
      String host,
      int port,
      Map<String, String> endpointTags) {

    Set<String> contentTypes =
        DataCodec.getAllInstances()
            .stream()
            .map(DataCodec::contentType)
            .collect(Collectors.toSet());

    List<ServiceRegistration> serviceRegistrations =
        serviceInstances
            .stream()
            .flatMap(
                serviceInfo ->
                    Arrays.stream(serviceInfo.serviceInstance().getClass().getInterfaces())
                        .map(
                            serviceInterface ->
                                new InterfaceInfo(serviceInterface, serviceInfo.tags())))
            .filter(
                interfaceInfo -> interfaceInfo.serviceInterface.isAnnotationPresent(Service.class))
            .map(
                interfaceInfo -> {
                  Class<?> serviceInterface = interfaceInfo.serviceInterface;
                  Map<String, String> serviceTags = interfaceInfo.tags;
                  String namespace = Reflect.serviceName(serviceInterface);
                  List<ServiceMethodDefinition> actions =
                      Arrays.stream(serviceInterface.getMethods())
                          .filter(method -> method.isAnnotationPresent(ServiceMethod.class))
                          .map(
                              method -> {
                                String action = Reflect.methodName(method);
                                CommunicationMode communicationMode =
                                    Reflect.communicationMode(method);
                                return new ServiceMethodDefinition(action, communicationMode);
                              })
                          .collect(Collectors.toList());
                  return new ServiceRegistration(namespace, serviceTags, actions);
                })
            .collect(Collectors.toList());

    return new ServiceEndpoint(
        endpointId, host, port, contentTypes, endpointTags, serviceRegistrations);
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
