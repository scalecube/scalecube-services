package io.scalecube.services.routing;


import static io.scalecube.services.routing.Routing.serviceLookup;
import static java.util.Collections.shuffle;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

import io.scalecube.services.ServiceHeaders;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;
import io.scalecube.transport.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
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

  private static final <T> Collector<T, List<T>, List<T>> toImmutableList() {
    return Collector.of(ArrayList::new, List::add,
        (left, right) -> {
          left.addAll(right);
          return left;
        }, Collections::unmodifiableList);
  }

  private static final Collector<?, ?, ?> SHUFFLER = collectingAndThen(
      toList(),
      list -> {
        shuffle(list);
        return list;
      });

  @SuppressWarnings("unchecked")
  public static <T> Collector<T, ?, List<T>> toShuffledList() {
    return (Collector<T, ?, List<T>>) SHUFFLER;
  }

  /**
   * Random selection router.
   * 
   * @return a router that randomly selects {@link ServiceInstance}
   */
  public static Routing random() {
    return (serviceRegistry, request) -> serviceLookup(serviceRegistry, request)
        .collect(collectingAndThen(toShuffledList(), Collections::unmodifiableList));
  }

  /**
   * Round-robin selection router.
   * 
   * @return a router that selects {@link ServiceInstance} in RoundRobin algorithm
   * @see RoundRobinServiceRouter
   */
  public static Routing roundRobin() {
    return new Routing() {
      final List<ServiceInstance> serviceLookup = new CopyOnWriteArrayList<>();
      Map<String, LongAdder> counters = new ConcurrentHashMap<>();

      @Override
      public Optional<ServiceInstance> route(ServiceRegistry serviceRegistry, Message request) {
        if (serviceLookup.isEmpty()) {
          routes(serviceRegistry, request);
        }

        LongAdder thisMethodCounter =
            counters.computeIfAbsent(request.header(ServiceHeaders.METHOD), (s) -> new LongAdder());
        if (serviceLookup.isEmpty()) {
          serviceLookup.addAll(routes(serviceRegistry, request));
          if (serviceLookup.isEmpty()) {
            return Optional.empty();
          }
        }

        thisMethodCounter.increment();
        int index = thisMethodCounter.intValue();
        if (index >= serviceLookup.size()) {
          thisMethodCounter.reset();
          index = index % serviceLookup.size();
        }
        return Optional.of(serviceLookup.get(index));
      }

      @Override
      public Collection<ServiceInstance> routes(ServiceRegistry serviceRegistry, Message request) {
        serviceLookup(serviceRegistry, request).filter(instance -> !serviceLookup.contains(instance))
            .forEach(serviceLookup::add);
        return unmodifiableList(serviceLookup);
      }

      @Override
      public String toString() {
        return "Round Robin Routing";
      }
    };
  }



  /**
   * A Weighted random router.
   * 
   * @return a router that balances {@link ServiceInstance}s with weighted random using the tag "Weight"
   */
  public static Routing weightedRandom() {

    return new Routing() {

      @Override
      public Optional<ServiceInstance> route(ServiceRegistry serviceRegistry, Message request) {
        RandomCollection<ServiceInstance> weightedRandom = new RandomCollection<>();

        serviceLookup(serviceRegistry, request).forEach(instance -> {
          weightedRandom.add(
              Double.valueOf(instance.tags().getOrDefault("Weight", "0")),
              instance);
        });
        return Optional.of(weightedRandom.next());
      }

      @Override
      public Collection<ServiceInstance> routes(ServiceRegistry serviceRegistry, Message request) {
        return serviceLookup(serviceRegistry, request).collect(toImmutableList());
      }
    };

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

  public static Routing withAllTags(Map<String, String> containsAll) {
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
  public static Routing withAllTags(Map<String, String> matchAll,
      BiPredicate<String, String> testMatch) {

    final Predicate<ServiceInstance> hasKeysWithValue =
        instance -> {
          if (instance.tags().keySet().containsAll(matchAll.keySet())) {
            AtomicReference<Boolean> result = new AtomicReference<>(true);
            matchAll.forEach((matchKey, matchValue) -> {
              result.accumulateAndGet(
                  testMatch.test(instance.tags().get(matchKey), matchValue),
                  Boolean::logicalAnd);
            });
            return result.get();
          } else {
            return false;
          }
        };

    return (ServiceRegistry serviceRegistry, Message request) -> serviceLookup(serviceRegistry, request)
        .filter(hasKeysWithValue).collect(toImmutableList());

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
  public static Routing withAllTagsMetchingRegex(Map<String, String> matchAll) {
    return withAllTags(matchAll, String::matches);
  }

  /**
   * Simple router for single key-value.
   * 
   * @param key the tag that the {@link ServiceInstance} should have.
   * @param value the value that must be equals the tag's value.
   * @return a router that routes only instances that has a tag with the given key-value pair.
   */
  public static Routing withTag(String key, String value) {

    final Predicate<ServiceInstance> hasKeyWithValue =
        instance -> {
          return value.equals(instance.tags().get(key));
        };

    return (ServiceRegistry serviceRegistry, Message request) -> serviceLookup(serviceRegistry, request)
        .filter(hasKeyWithValue).collect(toImmutableList());
  }
}
