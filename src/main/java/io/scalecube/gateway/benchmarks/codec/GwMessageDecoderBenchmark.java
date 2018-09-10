package io.scalecube.gateway.benchmarks.codec;

import io.netty.buffer.ByteBuf;
import io.scalecube.benchmarks.BenchmarkSettings;
import io.scalecube.benchmarks.metrics.BenchmarkTimer;
import io.scalecube.benchmarks.metrics.BenchmarkTimer.Context;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import io.scalecube.gateway.websocket.message.GatewayMessageCodec;
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
              ByteBuf bb = state.byteBufExample();
              BenchmarkTimer timer = state.timer("timer");

              return i -> {
                Context timerContext = timer.time();
                GatewayMessage gatewayMessage = codec.decode(bb);
                timerContext.stop();
                return gatewayMessage;
              };
            });
  }
}
