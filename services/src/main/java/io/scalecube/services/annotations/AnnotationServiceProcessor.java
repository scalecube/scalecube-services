package io.scalecube.services.annotations;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ProxyDefinition;

import com.google.common.base.Strings;

import java.lang.annotation.Annotation;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Field;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

public class AnnotationServiceProcessor implements ServiceProcessor {

  @Override
  public Collection<Class<?>> extractServiceInterfaces(Object service) {
    return extractServiceInterfaces(service.getClass());
  }

  @Override
  public Collection<Class<?>> extractServiceInterfaces(Class<?> serviceInterface) {
    Class<?>[] interfaces = serviceInterface.getInterfaces();
    return Arrays.stream(interfaces)
        .filter(interfaceClass -> interfaceClass.isAnnotationPresent(Service.class))
        .collect(Collectors.toList());
  }

  @Override
  public ServiceDefinition introspectServiceInterface(Class<?> serviceInterface) {
    // Service name
    Service serviceAnnotation = serviceInterface.getAnnotation(Service.class);
    checkArgument(serviceAnnotation != null, "Not a service interface: %s", serviceInterface);
    String serviceName = resolveServiceName(serviceInterface, serviceAnnotation);

    // Method name
    Map<String, Method> methods = Arrays.stream(serviceInterface.getMethods())
        .filter(method -> method.isAnnotationPresent(ServiceMethod.class))
        .collect(Collectors.toMap(method -> {
          ServiceMethod methodAnnotation = method.getAnnotation(ServiceMethod.class);
          return resolveMethodName(method, methodAnnotation);
        }, Function.identity()));

    return new ServiceDefinition(serviceInterface, serviceName, Collections.unmodifiableMap(methods));
  }

  private String resolveServiceName(Class<?> serviceInterface, Service serviceAnnotation) {
    return Strings.isNullOrEmpty(serviceAnnotation.value()) ? serviceInterface.getName() : serviceAnnotation.value();
  }

  private String resolveMethodName(Method method, ServiceMethod methodAnnotation) {
    return Strings.isNullOrEmpty(methodAnnotation.value()) ? method.getName() : methodAnnotation.value();
  }

  @Override
  public Set<ServiceDefinition> serviceDefinitions(Class<?> serviceInterface) {
    return this.extractServiceInterfaces(serviceInterface).stream()
        .map(foreach -> introspectServiceInterface(foreach))
        .collect(Collectors.toSet());
  }

  @Override
  public Collection<Class<?>> extractInjectableParameterFromConstructor(Constructor<?> constructor) {

    return Arrays.asList(constructor).stream()
        .map(construct -> construct.getParameterTypes())
        .flatMap(Arrays::stream).collect(Collectors.toList());
  }

  @Override
  public Collection<ProxyDefinition> extractServiceProxyFromConstructor(Constructor<?> constructor) {

    Annotation[][] annotations = constructor.getParameterAnnotations();
    Class<?>[] parameterTypes = constructor.getParameterTypes();
    int index = 0;
    List<ProxyDefinition> proxyList = new LinkedList<>();
    for (Annotation[] array : annotations) {
      Class<?> currentType = parameterTypes[index];
      proxyList.addAll(Arrays.asList(array).stream()
          .filter(annotation -> annotation.annotationType() == ServiceProxy.class)
          .map(annotation -> new ProxyDefinition(currentType, ((ServiceProxy) annotation).router(),
              Duration.ofMillis(((ServiceProxy) annotation).timeout()))).collect(Collectors.toList()));
      index++;
    }
    return proxyList;
  }

  @Override
  public Constructor<?> extractConstructorInjectables(Class<?> serviceImpl) {
    Constructor<?>[] constructors = serviceImpl.getDeclaredConstructors();
    Constructor<?> constructor = Arrays.asList(constructors).stream()
        .filter(construct -> construct.isAnnotationPresent(Inject.class)).findFirst().orElse(constructors[0]);
    return constructor;
  }

  @Override
  public Collection<Field> extractMemberInjectables(Class<?> serviceImpl) {
    Field[] fields = serviceImpl.getDeclaredFields();
    return Arrays.asList(fields).stream().filter(field -> field.isAnnotationPresent(Inject.class))
        .collect(Collectors.toList());
  }

  @Override
  public Collection<ProxyDefinition> extractServiceProxyFromMembers(Collection<Field> fields) {
    Collection<ProxyDefinition> proxyList = fields.stream()
        .filter(field -> field.isAnnotationPresent(ServiceProxy.class))
        .map(field -> new ProxyDefinition(field.getType(), field.getAnnotation(ServiceProxy.class).router(),
            Duration.ofMillis(field.getAnnotation(ServiceProxy.class).timeout()))).collect(Collectors.toList());
    return proxyList;
  }

  @Override
  public boolean isServiceInterface(Class<?> clsType) {
    return clsType.isAnnotationPresent(Service.class);
  }

}
