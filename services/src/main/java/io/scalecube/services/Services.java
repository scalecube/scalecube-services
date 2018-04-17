package io.scalecube.services;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

public class Services {

  private Collection<ServiceBuilder> services = Collections.EMPTY_SET;

  public Stream<ServiceBuilder> stream() {
    return services.stream();
  }

  public Collection<ServiceBuilder> list() {
    return services;
  }

  public static Services empty() {
    return new Services();
  }


}
