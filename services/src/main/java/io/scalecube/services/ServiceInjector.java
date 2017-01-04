package io.scalecube.services;

import java.lang.reflect.InvocationTargetException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;

import org.slf4j.LoggerFactory;

public class ServiceInjector {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ServiceInjector.class);

  private final Map<Class, Object> instances;
  private final ServiceProcessor serviceProcessor = new AnnotationServiceProcessor();

  private ServiceInjector(Map<Class, Object> instances) {
    this.instances = instances;
  }

  public <T> T getInstance(Microservices services, Class<T> cls) {
    Collection<Class<?>> injectables = serviceProcessor.extractInjectables(cls);
    injectables.stream().filter(srv -> serviceProcessor.isServiceInterface(srv))
            .forEach(srv -> resolveProxy(services, srv));
    Class<?>[] types = injectables.stream().toArray(size -> new Class<?>[size]);
    Object[] args = injectables.stream()
            .filter(paramType -> instances.containsKey(paramType))
            .map(paramType -> instances.get(paramType))
            .toArray(size -> new Object[size]);
    try {
      return cls.getConstructor(types).newInstance(args);
    } catch (NoSuchMethodException | SecurityException | InstantiationException 
            | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
      LOGGER.error("service instance [{}] initialization failed with exception [{}]", cls.getName(), ex);
      throw new RuntimeException(ex);
    }
  }

  public void resolveProxy(Microservices services, Class<?> serviceInterface) {
    instances.computeIfAbsent(serviceInterface, (srv) -> services.proxy().api(srv).create());
  }

  public static final class Builder {

    private final Map<Class, Object> instances = new HashMap<>();

    public <T> ClassBinder<? extends T> bind(Class<? extends T> cls) {
      return new ClassBinder<>(this, cls);
    }

    public ServiceInjector build() {
      return new ServiceInjector(instances);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class ClassBinder<T> {

    ServiceInjector.Builder injectorBuilder = null;
    Class<T> clazz;

    private ClassBinder(ServiceInjector.Builder injectorBuilder, Class<T> clazz) {
      this.injectorBuilder = injectorBuilder;
      this.clazz = clazz;
    }

    public ServiceInjector.Builder to(T instance) {
      injectorBuilder.instances.put(clazz, instance);
      return injectorBuilder;
    }
  }
}
