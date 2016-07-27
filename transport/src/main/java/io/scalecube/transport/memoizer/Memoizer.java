package io.scalecube.transport.memoizer;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

public class Memoizer<A, V> {

  private final ConcurrentMap<A, ListenableFuture<V>> cache = new ConcurrentHashMap<>();

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
   * @param defaultComputable the default computable
   */
  public Memoizer(Computable<A, V> defaultComputable) {
    this.defaultComputable = defaultComputable;
  }

  public V get(final A arg) throws MemoizerExecutionException {
    return get(arg, defaultComputable);
  }

  public V get(final A arg, final Executor executor) throws MemoizerExecutionException {
    return get(arg, defaultComputable, executor);
  }

  public V get(final A arg, final Computable<A, V> computable) throws MemoizerExecutionException {
    return get(arg, computable, MoreExecutors.directExecutor());
  }

  /**
   * Returns the value to which the specified key is mapped, or run computable to compute the value if absent. NOTE:
   * This is blocking call
   */
  public V get(final A arg, final Computable<A, V> computable, final Executor executor)
      throws MemoizerExecutionException {
    while (true) {
      ListenableFuture<V> future = getAsync(arg, computable, executor);
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

  public ListenableFuture<V> getAsync(final A arg, final Executor executor) {
    return getAsync(arg, defaultComputable, executor);
  }

  public ListenableFuture<V> getAsync(final A arg, final Computable<A, V> computable, final Executor executor) {
    ListenableFuture<V> future = cache.get(arg);
    if (future == null) {
      Callable<V> eval = new Callable<V>() {
        @Override
        public V call() throws Exception {
          Preconditions.checkArgument(computable != null, "Computable is null");
          return computable.compute(arg);
        }
      };
      final ListenableFutureTask<V> futureTask = ListenableFutureTask.create(eval);
      future = cache.putIfAbsent(arg, futureTask);
      if (future == null) {
        future = futureTask;
        executor.execute(new Runnable() {
          @Override
          public void run() {
            futureTask.run();
          }
        });
      }
    }
    return future;
  }

  public boolean isEmpty() {
    return cache.isEmpty();
  }

  /**
   * Returns the value to which the specified key is mapped, or null otherwise. NOTE: This is a blocking call
   */
  public V getIfExists(final A arg) throws MemoizerExecutionException {
    return containsKey(arg) ? get(arg) : null;
  }

  /**
   * Removes the value to which the specified key is mapped, or null otherwise. NOTE: This is a blocking call
   * 
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

  public void delete(A arg) {
    cache.remove(arg);
  }

  public boolean containsKey(A arg) {
    return cache.containsKey(arg);
  }

  public Set<A> keySet() {
    return cache.keySet();
  }

  public void clear() {
    cache.clear();
  }
}
