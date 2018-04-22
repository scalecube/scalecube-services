package io.scalecube.rsockets;

import io.scalecube.cluster.membership.IdGenerator;
import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceMethodDefinition;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.annotations.ContentType;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ServiceScanner {

    private static Logger LOGGER = LoggerFactory.getLogger(ServiceScanner.class);


    public static ServiceEndpoint scan(List<Class<?>> clazzs, String host, int port, Map<String, String> tags) {
        String endpointId = IdGenerator.generateId();
        List<ServiceRegistration> serviceRegistrations = clazzs.stream()
                .flatMap(c -> Arrays.stream(c.getInterfaces()))
                .filter(i -> i.isAnnotationPresent(Service.class))
                .map(serviceInterface -> {
                    String namespace = Reflect.serviceName(serviceInterface);
                    Map<String, String> serviceTags = serviceTags(serviceInterface);
                    ContentType ctAnnotation = serviceInterface.getAnnotation(ContentType.class);
                    String serviceContentType = ctAnnotation != null ? ctAnnotation.value() : ContentType.DEFAULT;
                    List<ServiceMethodDefinition> actions = Arrays.stream(serviceInterface.getMethods())
                            .filter(m -> m.isAnnotationPresent(ServiceMethod.class))
                            .map(m -> {

                                String action = Reflect.methodName(m);
                                String contentType = ContentType.DEFAULT;
                                Map<String, String> methodTags = methodTags(m);
                                String communicationMode = CommunicationMode.of(m).map(CommunicationMode::name).orElse("unsupported");
                                return new ServiceMethodDefinition(action, contentType, communicationMode, methodTags);
                            }).collect(Collectors.toList());
                    return new ServiceRegistration(namespace,
                            serviceContentType,
                            serviceTags,
                            actions);
                }).collect(Collectors.toList());
        return new ServiceEndpoint(endpointId, host, port, tags, serviceRegistrations);
    }

    public static List<ServiceReference> toServiceReferences(ServiceEndpoint e) {
//        Stream<ServiceMethodDefinition> serviceMethodDefinitionStream = e.serviceRegistrations().stream()
//                .map(sr -> sr.setTags(merge(sr.tags(), e.tags())))
//                .flatMap(sr ->
//                        sr.methods().stream()
//                                .map(m -> m.setTags(merge(sr.tags(), m.getTags())).setContentType(merge(sr.contentType(), m.getContentType()))));

        return null;
    }

    private static String merge(String lowPriority, String highPriority) {
        return highPriority == null ? lowPriority : highPriority;
    }

    private static Map<String, String> merge(Map<String, String> lowPriority, Map<String, String> highPriority) {
        Map<String, String> result = new HashMap<>();
        result.putAll(lowPriority);
        result.putAll(highPriority);
        return result;
    }

    private static Map<String, String> methodTags(Method m) {
        // TODO: tags are not yet implemented on API level
        return new HashMap<>();
    }

    private static Map<String, String> serviceTags(Class<?> serviceInterface) {
        // TODO: tags are not yet implemented on API level
        return new HashMap<>();
    }
}
