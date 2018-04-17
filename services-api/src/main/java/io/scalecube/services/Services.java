package io.scalecube.services;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

public class Services {

  private Collection<Service> services = Collections.EMPTY_SET;

  public Stream<Service> stream() {
    return services.stream();
  }

  public Collection<Service> list() {
    return services;
  }

  public static Services empty() {
    return new Services();
  }


}
