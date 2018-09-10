package io.scalecube.services.benchmarks.codec;

import io.netty.util.ReferenceCountUtil;
import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.BenchmarkState;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageCodec;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class SmFullEncodeBenchmarks {

  private SmFullEncodeBenchmarks() {
    // Do not instantiate
  }

  /**
   * Runner function for benchmarks.
   *
   * @param args program arguments
   * @param benchmarkStateFactory producer function for {@link BenchmarkState}
   */
  public static void runWith(
      String[] args, Function<BenchmarkSettings, SmCodecBenchmarksState> benchmarkStateFactory) {

    BenchmarkSettings settings =
        BenchmarkSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();

    SmCodecBenchmarksState benchmarkState = benchmarkStateFactory.apply(settings);

    benchmarkState.runForSync(
        state -> {
          BenchmarkTimer timer = state.timer("timer");
          ServiceMessageCodec messageCodec = state.messageCodec();
          ServiceMessage message = state.message();

          return i -> {
            Context timeContext = timer.time();
            Object result =
                messageCodec.encodeAndTransform(
                    message,
                    (dataByteBuf, headersByteBuf) -> {
                      ReferenceCountUtil.release(dataByteBuf);
                      ReferenceCountUtil.release(headersByteBuf);
                      return dataByteBuf;
                    });
            timeContext.stop();
            return result;
          };
        });
  }
}
