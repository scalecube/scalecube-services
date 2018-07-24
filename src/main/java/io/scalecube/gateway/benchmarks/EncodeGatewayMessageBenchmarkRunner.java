package io.scalecube.gateway.benchmarks;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.gateway.websocket.message.GatewayMessage;
import io.scalecube.gateway.websocket.message.GatewayMessageCodec;

import com.codahale.metrics.Timer;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.TimeUnit;

public class EncodeGatewayMessageBenchmarkRunner {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();
    new GatewayMessageCodecBenchmarkState(settings).runForSync(state -> {

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
