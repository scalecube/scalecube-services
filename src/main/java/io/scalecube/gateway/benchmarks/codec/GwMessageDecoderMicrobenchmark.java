package io.scalecube.gateway.benchmarks.codec;

import io.netty.buffer.ByteBuf;
import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.benchmarks.metrics.BenchmarksTimer;
import io.scalecube.benchmarks.metrics.BenchmarksTimer.Context;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import io.scalecube.gateway.websocket.message.GatewayMessageCodec;
import java.util.concurrent.TimeUnit;

public class GwMessageDecoderMicrobenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    BenchmarksSettings settings =
        BenchmarksSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();
    new GwMessageCodecMicrobenchmarkState(settings)
        .runForSync(
            state -> {
              GatewayMessageCodec codec = state.codec();
              ByteBuf bb = state.byteBufExample();
              BenchmarksTimer timer = state.timer("timer");

              return i -> {
                Context timerContext = timer.time();
                GatewayMessage gatewayMessage = codec.decode(bb);
                timerContext.stop();
                return gatewayMessage;
              };
            });
  }
}
