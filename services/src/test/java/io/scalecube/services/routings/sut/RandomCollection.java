package io.scalecube.services.routings.sut;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

public class RandomCollection<E> {
  private final NavigableMap<Double, E> map = new TreeMap<>();
  private double total = 0;

  /**
   * Add weighted result.
   *
   * @param weight weight
   * @param result result
   */
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
