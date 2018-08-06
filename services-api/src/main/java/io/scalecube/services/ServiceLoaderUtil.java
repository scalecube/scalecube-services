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
   * Find the first service loaded by a {@link ServiceLoader}.
   * 
   * @param clazz the service type
   */
  public static <T> Optional<T> findFirstMatched(Class<T> clazz) {
    return findFirst(clazz, (x) -> true);
  }

  /**
   * Find the first service loaded by a {@link ServiceLoader} that and match a predicate.
   * 
   * @param clazz the service type
   * @param predicate a test on the service
   * @return the first one that matches the predicate.
   */
  public static <T> Optional<T> findFirst(Class<T> clazz, Predicate<? super T> predicate) {
    ServiceLoader<T> load = ServiceLoader.load(clazz);
    Stream<T> stream = StreamSupport.stream(load.spliterator(), false);
    return stream.filter(predicate).findFirst();
  }
}
