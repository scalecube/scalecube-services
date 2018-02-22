package io.scalecube.services.routing;

import io.scalecube.services.ServiceHeaders;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;
import io.scalecube.transport.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collector;

/**
 * This class gives you some of the common {@link Router}s.
 * 
 * @author Aharon H.
 *
 */
public class Routers {

  private static final class RandomCollection<E> {
    private final NavigableMap<Double, E> map = new TreeMap<Double, E>();
    private double total = 0;

    public void add(double weight, E result) {
      if (weight > 0 && !map.containsValue(result)) {
        map.put(total += weight, result);
      }
    }

    public E next() {
      double value = ThreadLocalRandom.current().nextDouble() * total;
      return map.ceilingEntry(value).getValue();
    }
  }

  private static final BiPredicate<ServiceInstance, Message> methodExists =
      (instance, request) -> instance.methodExists(request.header(ServiceHeaders.METHOD));


  /**
   * Random selection router.
   * 
   * @return a router that randomly selects {@link ServiceInstance}
   */
  public static Class<? extends Router> random() {
    return RandomServiceRouter.class;
  }

  /**
   * Round-robin selection router.
   * 
   * @return a router that selects {@link ServiceInstance} in RoundRobin algorithm
   * @see RoundRobinServiceRouter
   */
  public static Class<? extends Router> roundRobin() {
    return RoundRobinServiceRouter.class;
  }

  private static final <T> Collector<T, List<T>, List<T>> toImmutableList() {
    return Collector.of(ArrayList::new, List::add,
        (left, right) -> {
          left.addAll(right);
          return left;
        }, Collections::unmodifiableList);
  }

  /**
   * A Weighted random router.
   * 
   * @return a router that balances {@link ServiceInstance}s with weighted random using the tag "Weight"
   */
  public static Class<? extends Router> weightedRandom() {

    class WeightedRandom implements Router {

      private ServiceRegistry serviceRegistry;

      @SuppressWarnings("unused")
      public WeightedRandom(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
      }

      @Override
      public Optional<ServiceInstance> route(Message request) {
        String serviceName = request.header(ServiceHeaders.SERVICE_REQUEST);
        RandomCollection<ServiceInstance> weightedRandom = new RandomCollection<>();
        serviceRegistry.serviceLookup(serviceName).stream().forEach(instance -> {
          weightedRandom.add(
              Double.valueOf(instance.tags().get("Weight")),
              instance);
        });
        return Optional.of(weightedRandom.next());
      }

      @Override
      public Collection<ServiceInstance> routes(Message request) {
        String serviceName = request.header(ServiceHeaders.SERVICE_REQUEST);
        return Collections.unmodifiableCollection(serviceRegistry.serviceLookup(serviceName));
      }
    }

    return WeightedRandom.class;
  }

  /**
   * Router for multiple key-value.
   * 
   * @param containsAll a map with all tags that the {@link ServiceInstance} should have.
   * 
   * @return a router that routes only instances that has all {@link ServiceInstance#tags() tags} on the map's key and
   *         which are {@link String#equals(Object) equal} to the corresponding value
   * @see Routers#withAllTags(Map, BiPredicate)
   */

  public static Class<? extends Router> withAllTags(Map<String, String> containsAll) {
    return withAllTags(containsAll, String::equals);
  }

  /**
   * Router for multiple key-value.
   * 
   * @param matchAll a map with all tags that the {@link ServiceInstance} should have.
   * @param testMatch a predicate for which the values in the map are tested against the actual values in the
   *        {@link ServiceInstance#tags()}
   * @return a router that routes only instances that has all of the tags that matches the predicate
   * @see Routers#withAllTags(Map)
   * @implNote the evaluation is always done on all keys
   */
  public static Class<? extends Router> withAllTags(Map<String, String> matchAll,
      BiPredicate<String, String> testMatch) {

    final Predicate<ServiceInstance> hasKeysWithValue =
        instance -> {
          if (instance.tags().keySet().containsAll(matchAll.keySet())) {
            AtomicReference<Boolean> result = new AtomicReference<>(true);
            matchAll.forEach((matchKey, matchValue) -> {
              result.accumulateAndGet(
                  testMatch.test(instance.tags().get(matchKey), matchValue), Boolean::logicalAnd);
            });
            return result.get();
          } else {
            return false;
          }
        };

    class RouterWithTags implements Router {
      private final ServiceRegistry serviceRegistry;

      @SuppressWarnings("unused")

      public RouterWithTags(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
      }

      @Override
      public Optional<ServiceInstance> route(Message request) {
        return routes(request).stream().unordered().findFirst();
      }

      @Override
      public Collection<ServiceInstance> routes(Message request) {
        String serviceName = request.header(ServiceHeaders.SERVICE_REQUEST);
        Predicate<ServiceInstance> hasMethod = instance -> Routers.methodExists.test(instance, request);
        return serviceRegistry.serviceLookup(serviceName).stream()
            .filter(hasMethod.and(hasKeysWithValue)).collect(toImmutableList());
      }
    }

    return RouterWithTags.class;
  }

  /**
   * Router for multiple key-value.
   * 
   * @param matchAll a map with all tags that the {@link ServiceInstance} should have.
   * 
   * @return a router that routes only instances that has all {@link ServiceInstance#tags() tags} on the map's key and
   *         which {@link String#matches(String) match} the corresponding value's regular expression
   * @see Routers#withAllTags(Map, BiPredicate)
   */
  public static Class<? extends Router> withAllTagsMetchingRegex(Map<String, String> matchAll) {
    return withAllTags(matchAll, String::matches);
  }

  /**
   * Simple router for single key-value.
   * 
   * @param key the tag that the {@link ServiceInstance} should have.
   * @param value the value that must be equals the tag's value.
   * @return a router that routes only instances that has a tag with the given key-value pair.
   */
  public static Class<? extends Router> withTag(String key, String value) {

    final Predicate<ServiceInstance> hasKeyWithValue =
        instance -> value.equals(instance.tags().get(key));

    class RouterWithTag implements Router {
      private final ServiceRegistry serviceRegistry;

      @SuppressWarnings("unused")
      public RouterWithTag(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
      }

      @Override
      public Optional<ServiceInstance> route(Message request) {
        return routes(request).stream().unordered().findFirst();
      }

      @Override
      public Collection<ServiceInstance> routes(Message request) {
        String serviceName = request.header(ServiceHeaders.SERVICE_REQUEST);
        Predicate<ServiceInstance> hasMethod = instance -> methodExists.test(instance, request);
        return serviceRegistry.serviceLookup(serviceName).stream()
            .filter(hasMethod.and(hasKeyWithValue)).collect(toImmutableList());
      }
    }

    return RouterWithTag.class;
  }
}
