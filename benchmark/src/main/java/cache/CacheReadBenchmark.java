package cache;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

@Fork(2)
@State(Scope.Benchmark)
@Threads(4)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CacheReadBenchmark {

  static final String[] GENERIC_ARGS = new String[] {"java.lang.String", "java.lang.Long", "java.lang.Integer",
      "java.lang.Short", "java.lang.Character", "java.lang.Byte", "io.scalecube.transport.protocol.MessageSchema",};

  static final String[] GENERIC_ARGS_CACHE_MISS = new String[] {"java.lang.String", "xyz", "java.lang.Integer", "abc",
      "java.lang.Character", "qwerty", "io.scalecube.transport.protocol.MessageSchema",};

  LoadingCache<String, Optional<Class>> guavaCache = CacheBuilder.newBuilder().build(
      new CacheLoader<String, Optional<Class>>() {
        @Override
        public Optional<Class> load(@Nonnull String className) {
          try {
            Class dataClass = Class.forName(className);
            return Optional.of(dataClass);
          } catch (ClassNotFoundException e) {
            return Optional.absent();
          }
        }
      });

  @Benchmark
  public void readFromGuavaCache() {
    for (String arg : GENERIC_ARGS) {
      guavaCache.getUnchecked(arg);
    }
  }

  @Benchmark
  public void readFromGuavaWithCacheMiss() {
    for (String arg : GENERIC_ARGS_CACHE_MISS) {
      guavaCache.getUnchecked(arg);
    }
  }

}
