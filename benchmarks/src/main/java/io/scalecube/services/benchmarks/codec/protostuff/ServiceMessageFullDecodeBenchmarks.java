package io.scalecube.services.benchmarks.codec.protostuff;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.benchmarks.codec.ServiceMessageCodecBenchmarksState;
import io.scalecube.services.codec.ServiceMessageCodec;

import com.codahale.metrics.Timer;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.TimeUnit;

public class ServiceMessageFullDecodeBenchmarks {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();
    new ServiceMessageCodecBenchmarksState.Protostuff(settings).runForSync(state -> {

      Timer timer = state.timer("timer");
      ServiceMessageCodec messageCodec = state.jacksonMessageCodec();
      Class<?> dataType = state.dataType();

      return i -> {
        Timer.Context timeContext = timer.time();
        ByteBuf dataBuffer = state.dataBuffer().retain();
        ByteBuf headersBuffer = state.headersBuffer().retain();
        ServiceMessage message =
            ServiceMessageCodec.decodeData(messageCodec.decode(dataBuffer, headersBuffer), dataType);
        timeContext.stop();
        return message;
      };
    });
  }
}
