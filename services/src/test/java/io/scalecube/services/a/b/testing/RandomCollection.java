package io.scalecube.services.a.b.testing;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

public class RandomCollection<E> {
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
