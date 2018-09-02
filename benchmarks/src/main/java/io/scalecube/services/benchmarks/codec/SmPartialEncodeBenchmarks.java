package io.scalecube.services.benchmarks.codec;

import io.netty.util.ReferenceCountUtil;
import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.BenchmarksState;
import io.scalecube.benchmarks.metrics.BenchmarksTimer;
import io.scalecube.benchmarks.metrics.BenchmarksTimer.Context;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageCodec;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class SmPartialEncodeBenchmarks {

  private SmPartialEncodeBenchmarks() {
    // Do not instantiate
  }

  /**
   * Runner function for benchmarks.
   *
   * @param args program arguments
   * @param benchmarkStateFactory producer function for {@link BenchmarksState}
   */
  public static void runWith(
      String[] args, Function<BenchmarksSettings, SmCodecBenchmarksState> benchmarkStateFactory) {

    BenchmarksSettings settings =
        BenchmarksSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();

    SmCodecBenchmarksState benchmarkState = benchmarkStateFactory.apply(settings);

    benchmarkState.runForSync(
        state -> {
          BenchmarksTimer timer = state.timer("timer");
          ServiceMessageCodec messageCodec = state.messageCodec();
          ServiceMessage message = state.messageWithByteBuf();

          return i -> {
            Context timeContext = timer.time();
            Object result =
                messageCodec.encodeAndTransform(
                    message,
                    (dataByteBuf, headersByteBuf) -> {
                      ReferenceCountUtil.release(headersByteBuf);
                      return dataByteBuf;
                    });
            timeContext.stop();
            return result;
          };
        });
  }
}
