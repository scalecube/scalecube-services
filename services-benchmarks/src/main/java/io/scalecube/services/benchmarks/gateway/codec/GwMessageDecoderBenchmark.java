package io.scalecube.services.benchmarks.gateway.codec;

import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkMeter;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import io.scalecube.services.gateway.ReferenceCountUtil;
import io.scalecube.services.gateway.ws.GatewayMessage;
import io.scalecube.services.gateway.ws.GatewayMessageCodec;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class GwMessageDecoderBenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    BenchmarkSettings settings =
        BenchmarkSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();

    new GwMessageCodecBenchmarkState(settings)
        .runForSync(
            state -> {
              GatewayMessageCodec codec = state.codec();
              BenchmarkTimer timer = state.timer("timer");
              BenchmarkMeter meter = state.meter("meter");

              return i -> {
                Context timerContext = timer.time();
                GatewayMessage message = codec.decode(state.byteBufExample().retain());
                Optional.ofNullable(message.data()) //
                    .ifPresent(ReferenceCountUtil::safestRelease);
                timerContext.stop();
                meter.mark();
                return message;
              };
            });
  }
}
