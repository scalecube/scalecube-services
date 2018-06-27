package io.scalecube.services.benchmarks.codecs;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageCodec;

import com.codahale.metrics.Timer;

import io.rsocket.Payload;
import io.rsocket.util.ByteBufPayload;

import java.util.concurrent.TimeUnit;

public class ServiceMessageEncodeBenchmarksRunner {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();
    new ServiceMessageCodecBenchmarkState(settings).runForSync(state -> {

      Timer timer = state.timer("timer");
      ServiceMessageCodec codec = state.codec();
      ServiceMessage message = state.message();

      return i -> {
        Timer.Context timeContext = timer.time();
        Payload payload = codec.encodeAndTransform(message, ByteBufPayload::create);
        payload.release();
        timeContext.stop();
        return payload;
      };
    });
  }
}
