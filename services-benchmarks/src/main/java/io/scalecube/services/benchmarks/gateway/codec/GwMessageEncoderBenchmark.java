package io.scalecube.services.benchmarks.gateway.codec;

import io.netty.buffer.ByteBuf;
import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkMeter;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import io.scalecube.services.gateway.ws.GatewayMessage;
import io.scalecube.services.gateway.ws.GatewayMessageCodec;
import java.util.concurrent.TimeUnit;

public class GwMessageEncoderBenchmark {

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
              GatewayMessage message = state.message();
              BenchmarkTimer timer = state.timer("timer");
              BenchmarkMeter meter = state.meter("meter");

              return i -> {
                Context timerContext = timer.time();
                ByteBuf bb = codec.encode(message);
                timerContext.stop();
                meter.mark();
                bb.release();
                return bb;
              };
            });
  }
}
