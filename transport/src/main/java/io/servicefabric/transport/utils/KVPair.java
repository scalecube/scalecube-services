package io.servicefabric.transport.utils;

/**
 * Generic class that represent key-value pair of two different values. Two pair is equal when both its key and value are equal. It is
 * immutable.
 *
 * @param <K> type of key
 * @param <V> type of value
 */
public class KVPair<K, V> {

  private final K key;
  private final V value;

  public KVPair(K key, V value) {
    this.key = key;
    this.value = value;
  }

  /**
   *
   * @return first value
   */
  public K getKey() {
    return key;
  }

  /**
   *
   * @return second value
   */
  public V getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    KVPair pair = (KVPair) o;

    if (key != null ? !key.equals(pair.key) : pair.key != null)
      return false;
    if (value != null ? !value.equals(pair.value) : pair.value != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "KVPair{" + "key=" + key + ", value=" + value + '}';
  }
}
