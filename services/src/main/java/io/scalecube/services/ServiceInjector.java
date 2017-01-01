package io.scalecube.services;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ServiceInjector {

  private final Map<Class, Object> clsInstMap;
  private final Set<Class> proxies;

  private ServiceInjector(Map<Class, Object> clsInstMap, Set<Class> proxies) {
    this.clsInstMap = clsInstMap;
    this.proxies = proxies;
  }

  public static class Builder {

    private final Map<Class, Object> clsInstMap = new HashMap<>();
    private final Set<Class> proxies = new HashSet<>();

    public <T> ClassBinder<? extends T> bind(Class<? extends T> cls) {
      return new ClassBinder<>(this, cls);
    }

    public ServiceInjector.Builder bindProxy(Class cls) {
      proxies.add(cls);
      return this;
    }

    public ServiceInjector build() {
      return new ServiceInjector(clsInstMap, proxies);
    }
  }

  public static class ClassBinder<T> {

    ServiceInjector.Builder injectorBuilder = null;
    Class<T> cls;

    private ClassBinder(ServiceInjector.Builder injectorBuilder, Class<T> cls) {
      this.injectorBuilder = injectorBuilder;
      this.cls = cls;
    }

    public ServiceInjector.Builder to(T inst) {
      injectorBuilder.clsInstMap.put(cls, inst);
      return injectorBuilder;
    }
  }
}
