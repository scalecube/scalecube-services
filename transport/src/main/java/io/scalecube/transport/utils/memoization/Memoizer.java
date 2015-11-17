package io.scalecube.transport.utils.memoization;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class Memoizer<A, V> {

  private final ConcurrentMap<A, Future<V>> cache = new ConcurrentHashMap<>();

  private final Computable<A, V> defaultComputable;

  /**
   * Instantiates a new memoizer without default computable.
   */
  public Memoizer() {
    this(null);
  }

  /**
   * Instantiates a new memoizer with the given default computable.
   *
   * @param computable the default computable
   */
  public Memoizer(Computable<A, V> computable) {
    this.defaultComputable = computable;
  }

  public V get(final A arg) throws MemoizerExecutionException {
    return get(arg, defaultComputable);
  }

  /**
   * Returns the value to which the specified key is mapped,
   * or run computable to compute the value if absent.
   * NOTE: This is blocking call
   */
  public V get(final A arg, final Computable<A, V> computable) throws MemoizerExecutionException {
    checkArgument(computable != null, "the computable can't be null");
    while (true) {
      Future<V> future = cache.get(arg);
      if (future == null) {
        Callable<V> eval = new Callable<V>() {
          public V call() throws Exception {
            return computable.compute(arg);
          }
        };
        FutureTask<V> futureTask = new FutureTask<>(eval);
        future = cache.putIfAbsent(arg, futureTask);
        if (future == null) {
          future = futureTask;
          futureTask.run();
        }
      }
      try {
        return future.get();
      } catch (CancellationException | InterruptedException e) {
        cache.remove(arg, future);
      } catch (ExecutionException e) {
        cache.remove(arg, future);
        throw new MemoizerExecutionException("Failed to compute value for key: " + arg + " with computable: "
            + computable, e.getCause());
      }
    }
  }

  public boolean isEmpty() {
    return cache.isEmpty();
  }

  /**
   * Returns the value to which the specified key is mapped,
   * or null otherwise.
   * NOTE: This is blocking call
   */
  public V getIfExists(final A arg) {
    Future<V> future = cache.get(arg);
    if (future != null) {
      try {
        return future.get();
      } catch (InterruptedException | ExecutionException ignore) {
        // ignore
      }
    }
    return null;
  }
  /**
   * Removes the value to which the specified key is mapped,
   * or null otherwise.
   * NOTE: This is blocking call
   * @return removed value if present, null otherwise
   */
  public V remove(A arg) {
    Future<V> future = cache.remove(arg);
    V res = null;
    if (future != null) {
      try {
        res = future.get();
      } catch (InterruptedException | ExecutionException ignore) {
        // ignore
      }
    }
    return res;
  }

  public boolean containsKey(A arg) {
    return cache.containsKey(arg);
  }

  public Set<A> keySet() {
    return cache.keySet();
  }
}
