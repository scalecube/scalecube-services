package test;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.openjdk.jmh.annotations.*;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import io.servicefabric.transport.utils.memoization.Computable;
import io.servicefabric.transport.utils.memoization.ConcurrentMapMemoizer;

@Fork(2)
@State(Scope.Thread)
@Threads(4)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CacheReadBenchmark {

	static final String[] GENERIC_ARGS = new String[] {
			"java.lang.String",
			"java.lang.Long",
			"java.lang.Integer",
			"java.lang.Short",
			"java.lang.Character",
			"java.lang.Byte",
			"io.servicefabric.transport.protocol.MessageSchema",
	};

	LoadingCache<String, Optional<Class>> guavaCache = CacheBuilder.newBuilder()
			.expireAfterAccess(1, TimeUnit.HOURS)
			.build(new CacheLoader<String, Optional<Class>>() {
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

	ConcurrentMapMemoizer<String, Optional<Class>> memoizer = new ConcurrentMapMemoizer<>(new Computable<String, Optional<Class>>() {
		@Override
		public Optional<Class> compute(String className) {
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
	public void readFromMemoizer() {
		for (String arg : GENERIC_ARGS) {
			memoizer.get(arg);
		}
	}
}
