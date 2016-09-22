package io.scalecube.transport.memoizer;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
import rx.functions.Func1;

import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;

public class Memoizer<A, V> {

  private final ConcurrentMap<A, CompletableFuture<V>> cache = new ConcurrentHashMap<>();

  private final Function<A, V> defaultComputable;

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
  public Memoizer(Function<A, V> defaultComputable) {
    this.defaultComputable = defaultComputable;
  }

  public V get(final A arg) throws MemoizerExecutionException {
    return get(arg, defaultComputable);
  }

  public V get(final A arg, final Executor executor) throws MemoizerExecutionException {
    return get(arg, defaultComputable, executor);
  }

  public V get(final A arg, final Function<A, V> computable) throws MemoizerExecutionException {
    return get(arg, computable, Runnable::run);
  }

  /**
   * Returns the value to which the specified key is mapped, or run computable to compute the value if absent. NOTE:
   * This is blocking call
   */
  public V get(final A arg, final Function<A, V> computable, final Executor executor)
      throws MemoizerExecutionException {
    while (true) {
      Future<V> future = getAsync(arg, computable, executor);
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

  public CompletableFuture<V> getAsync(final A arg, final Executor executor) {
    return getAsync(arg, defaultComputable, executor);
  }

  /**
   * Returns the value's future for the the specified key. It will run computable to compute the value if absent
   * asynchronously at the given executor.
   */
  public CompletableFuture<V> getAsync(final A arg, final Function<A, V> computable, final Executor executor) {
    CompletableFuture<V> future = cache.get(arg);
    if (future == null) {
      final CompletableFuture<V> compute = new CompletableFuture<>();
      future = cache.putIfAbsent(arg, compute);
      if (future == null) {
        future = CompletableFuture.supplyAsync(() -> computable.apply(arg), executor)
            .thenCompose(x -> {
              compute.complete(x);
              return compute;
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
