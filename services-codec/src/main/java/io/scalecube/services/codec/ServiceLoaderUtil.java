package io.scalecube.services.codec;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class ServiceLoaderUtil {

  private ServiceLoaderUtil() {
    // Do not instantiate
  }

  public static <T> Optional<T> findFirst(Class<T> aClass, Predicate<? super T> predicate) {
    ServiceLoader<T> load = ServiceLoader.load(aClass);
    Stream<T> stream = StreamSupport.stream(load.spliterator(), false);
    return stream.filter(predicate).findFirst();
  }
}
