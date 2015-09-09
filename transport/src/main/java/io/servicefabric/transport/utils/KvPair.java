package io.servicefabric.transport.utils;

/**
 * Generic class that represent key-value pair of two different values. Two pair is equal when both its key and value are equal. It is
 * immutable.
 *
 * @param <K> type of key
 * @param <V> type of value
 */
public class KvPair<K, V> {

  private final K key;
  private final V value;

  public KvPair(K key, V value) {
    this.key = key;
    this.value = value;
  }


  public K getKey() {
    return key;
  }


  public V getValue() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    KvPair pair = (KvPair) other;

    if (key != null ? !key.equals(pair.key) : pair.key != null) {
      return false;
    }
    if (value != null ? !value.equals(pair.value) : pair.value != null) {
      return false;
    }

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
