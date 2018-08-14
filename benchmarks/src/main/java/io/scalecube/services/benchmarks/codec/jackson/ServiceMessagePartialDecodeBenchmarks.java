package io.scalecube.services.benchmarks.codec.jackson;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.benchmarks.codec.ServiceMessageCodecBenchmarksState;
import io.scalecube.services.codec.ServiceMessageCodec;

import com.codahale.metrics.Timer;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.TimeUnit;

public class ServiceMessagePartialDecodeBenchmarks {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();
    new ServiceMessageCodecBenchmarksState.Jackson(settings).runForSync(state -> {

      Timer timer = state.timer("timer");
      ServiceMessageCodec messageCodec = state.jacksonMessageCodec();

      return i -> {
        Timer.Context timeContext = timer.time();
        ByteBuf dataBuffer = state.dataBuffer().retain();
        ByteBuf headersBuffer = state.headersBuffer().retain();
        ServiceMessage message = messageCodec.decode(dataBuffer, headersBuffer);
        ReferenceCountUtil.release(message.data());
        timeContext.stop();
        return message;
      };
    });
  }
}
