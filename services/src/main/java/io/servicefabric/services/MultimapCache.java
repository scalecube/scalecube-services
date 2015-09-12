package io.servicefabric.services;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

public class MultimapCache<K, V> {

  private ConcurrentMap<K, Collection<V>> cache = Maps.newConcurrentMap();

  public Collection<V> get(K key) {
    final Collection<V> serviceReferences = cache.get(key);
    return serviceReferences == null ? Collections.<V>emptyList() : Collections
        .unmodifiableCollection(serviceReferences);
  }

  public boolean put(K key, V value) {
    Collection<V> services = cache.get(key);
    if (services == null) {
      services = Sets.newConcurrentHashSet();
      final Collection<V> fasterServices = cache.putIfAbsent(key, services);
      if (fasterServices != null) {
        services = fasterServices;
      }
    }
    return services.add(value);
  }

  public boolean remove(K key, V value) {
    Collection<V> set = cache.get(key);
    return set != null && set.remove(value);
  }

  public void clear() {
    cache.clear();
  }

}
