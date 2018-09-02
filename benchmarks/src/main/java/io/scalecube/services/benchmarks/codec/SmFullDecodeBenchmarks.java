package io.scalecube.services.benchmarks.codec;

import io.netty.buffer.ByteBuf;
import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.BenchmarksState;
import io.scalecube.benchmarks.metrics.BenchmarksTimer;
import io.scalecube.benchmarks.metrics.BenchmarksTimer.Context;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageCodec;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class SmFullDecodeBenchmarks {

  private SmFullDecodeBenchmarks() {
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
          Class<?> dataType = state.dataType();

          return i -> {
            Context timeContext = timer.time();
            ByteBuf dataBuffer = state.dataBuffer().retain();
            ByteBuf headersBuffer = state.headersBuffer().retain();
            ServiceMessage message =
                ServiceMessageCodec.decodeData(
                    messageCodec.decode(dataBuffer, headersBuffer), dataType);
            timeContext.stop();
            return message;
          };
        });
  }
}
