package io.scalecube.gateway.benchmarks.codec;

import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import io.scalecube.gateway.websocket.message.GatewayMessageCodec;
import java.util.concurrent.TimeUnit;

public class GWMessageEncoderMicrobenchmark {

  /**
   * Main runner.
   *
   * @param args program arguments
   */
  public static void main(String[] args) {
    BenchmarksSettings settings =
      BenchmarksSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();
    new GWMessageCodecMicrobenchmarkState(settings)
      .runForSync(
        state -> {
          GatewayMessageCodec codec = state.codec();
          GatewayMessage message = state.message();
          Timer timer = state.timer("timer");

          return i -> {
            Timer.Context timerContext = timer.time();
            ByteBuf bb = codec.encode(message);
            timerContext.stop();
            bb.release();
            return bb;
          };
        });
  }
}
