package io.scalecube.services;

import io.scalecube.services.annotations.AnnotationServiceProcessor;
import io.scalecube.services.annotations.ServiceProcessor;

import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Field;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ServiceInjector {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ServiceInjector.class);

  private final Map<Class, Object> instances;
  private final ServiceProcessor serviceProcessor = new AnnotationServiceProcessor();

  private ServiceInjector(Map<Class, Object> instances) {
    this.instances = instances;
  }

  public static ServiceInjector defaultInstance() {
    return ServiceInjector.builder().build();
  }

  /**
   * get service instance created by injector with all dependencies populated.
   *
   * @param <T> service type
   * @param services service instance
   * @param cls service class
   * @return service instance
   */
  public <T> T getInstance(Microservices services, Class<T> cls) {
    Constructor<?> constructor = serviceProcessor.extractConstructorInjectables(cls);
    Collection<ProxyDefinition> proxyDefs = serviceProcessor.extractServiceProxyFromConstructor(constructor);
    proxyDefs.stream().forEach(proxyDef->resolveProxy(services, proxyDef));
    
    Collection<Class<?>> injectables = serviceProcessor.extractInjectableParameterFromConstructor(constructor);
    injectables.stream().filter(srv -> serviceProcessor.isServiceInterface(srv))
        .forEach(srv -> resolveProxy(services, new ProxyDefinition(srv)));
    Class<?>[] types = injectables.stream().toArray(size -> new Class<?>[size]);
    Object[] args = injectables.stream()
        .filter(paramType -> instances.containsKey(paramType))
        .map(paramType -> instances.get(paramType))
        .toArray(size -> new Object[size]);

    try {
      T instance;
      if (types.length != 0) {
        instance = cls.getConstructor(types).newInstance(args);
      } else {
        instance = cls.newInstance();
      }

      injectMembers(services, instance);
      return instance;
    } catch (NoSuchMethodException | SecurityException | InstantiationException
        | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
      LOGGER.error("service instance [{}] initialization failed with exception [{}]", cls.getName(), ex);
      throw new ServiceInjectorException(ex);
    }
  }

  private <T> void injectMembers(Microservices services, T instance) {
    Collection<Field> injectables = serviceProcessor.extractMemberInjectables(instance.getClass());
    Collection<ProxyDefinition> proxyDefs = serviceProcessor.extractServiceProxyFromMembers(injectables);
    proxyDefs.stream().forEach(proxyDefinition->resolveProxy(services, proxyDefinition));
    
    injectables.stream().filter(field -> serviceProcessor.isServiceInterface(field.getType()))
        .map(field -> field.getType()).forEach(srv -> resolveProxy(services, new ProxyDefinition(srv)));
    injectables.stream().forEach(field -> injectMember(field, instance));
  }

  private <T> void injectMember(Field field, T instance) {
    field.setAccessible(true);
    try {
      field.set(instance, instances.get(field.getType()));
    } catch (IllegalArgumentException | IllegalAccessException ex) {
      LOGGER.error("service instance member [{}] inject failed with exception [{}]", field.getName(), ex);
      throw new ServiceInjectorException(ex);
    }
  }

  private void resolveProxy(Microservices services, ProxyDefinition proxyDefinition) {

    instances.computeIfAbsent(proxyDefinition.getServiceInterface(), (srv) -> createProxy(services, proxyDefinition));

  }

  private <T> T createProxy(Microservices services, ProxyDefinition proxyDefinition) {
    Microservices.ProxyContext proxyContext = services.proxy().api(proxyDefinition.getServiceInterface());
    if (proxyDefinition.getRouter() != null) {
      proxyContext.router(proxyDefinition.getRouter());
    }
    if (!proxyDefinition.getDuration().isZero()) {
      proxyContext.timeout(proxyDefinition.getDuration());
    }
    return proxyContext.create();
  }

  public static final class Builder {

    private final Map<Class, Object> instances = new HashMap<>();

    public <T> ClassBinder<T> bind(Class<T> cls) {
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

    private ServiceInjector.Builder injectorBuilder = null;
    private Class<T> clazz;

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
