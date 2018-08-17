package io.scalecube.services;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class ServiceLoaderUtil {

  private ServiceLoaderUtil() {
    // Do not instantiate
  }

  /**
   * Finds the first implementation of the given service type and creates its instance.
   *
   * @param clazz service type
   * @return the first implementation of the given service type
   */
  public static <T> Optional<T> findFirst(Class<T> clazz) {
    ServiceLoader<T> load = ServiceLoader.load(clazz);
    return StreamSupport.stream(load.spliterator(), false).findFirst();
  }

  /**
   * Finds the first implementation of the given service type using the given predicate to filter
   * out found service types and creates its instance.
   *
   * @param clazz service type
   * @param predicate service type predicate
   * @return the first implementation of the given service type
   */
  public static <T> Optional<T> findFirst(Class<T> clazz, Predicate<? super T> predicate) {
    ServiceLoader<T> load = ServiceLoader.load(clazz);
    Stream<T> stream = StreamSupport.stream(load.spliterator(), false);
    return stream.filter(predicate).findFirst();
  }

  /**
   * Finds all implementations of the given service type and creates their instances.
   *
   * @param clazz service type
   * @return implementations' stream of the given service type
   */
  public static <T> Stream<T> findAll(Class<T> clazz) {
    ServiceLoader<T> load = ServiceLoader.load(clazz);
    return StreamSupport.stream(load.spliterator(), false);
  }

  /**
   * Finds all implementations of the given service type using the given predicate to filter out
   * found service types and creates their instances.
   *
   * @param clazz service type
   * @param predicate service type predicate
   * @return implementations' stream of the given service type
   */
  public static <T> Stream<T> findAll(Class<T> clazz, Predicate<? super T> predicate) {
    ServiceLoader<T> load = ServiceLoader.load(clazz);
    return StreamSupport.stream(load.spliterator(), false).filter(predicate);
  }
}
