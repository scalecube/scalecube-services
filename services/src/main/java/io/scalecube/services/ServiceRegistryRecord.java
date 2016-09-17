package io.scalecube.services;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author Anton Kharenko
 */
@Immutable
public class ServiceRegistryRecord {

  private final Set<ServiceReference> services;

  private ServiceRegistryRecord() {
    this.services = new HashSet<>();
  }

  private ServiceRegistryRecord(Set<ServiceReference> services) {
    checkArgument(services != null);
    this.services = services;
  }

  public static ServiceRegistryRecord newInstance() {
    return new ServiceRegistryRecord();
  }

  public ServiceRegistryRecord add(ServiceReference service) {
    HashSet<ServiceReference> servicesCopy = new HashSet<>(services);
    servicesCopy.add(service);
    return new ServiceRegistryRecord(servicesCopy);
  }

  public ServiceRegistryRecord addAll(Collection<ServiceReference> services) {
    HashSet<ServiceReference> servicesCopy = new HashSet<>(this.services);
    for (ServiceReference service : services) {
      servicesCopy.add(service);
    }
    return new ServiceRegistryRecord(servicesCopy);
  }

  public ServiceRegistryRecord remove(ServiceReference service) {
    HashSet<ServiceReference> servicesCopy = new HashSet<>(services);
    servicesCopy.remove(service);
    return new ServiceRegistryRecord(servicesCopy);
  }

  public ServiceRegistryRecord removeAll(Collection<ServiceReference> services) {
    HashSet<ServiceReference> servicesCopy = new HashSet<>(services);
    for (ServiceReference service : services) {
      servicesCopy.remove(service);
    }
    return new ServiceRegistryRecord(servicesCopy);
  }

  public boolean contains(ServiceReference service) {
    return services.contains(service);
  }

  public Set<ServiceReference> services() {
    return Collections.unmodifiableSet(services);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServiceRegistryRecord that = (ServiceRegistryRecord) o;
    return Objects.equals(services, that.services);
  }

  @Override
  public int hashCode() {
    return Objects.hash(services);
  }

  @Override
  public String toString() {
    return services.toString();
  }
}
