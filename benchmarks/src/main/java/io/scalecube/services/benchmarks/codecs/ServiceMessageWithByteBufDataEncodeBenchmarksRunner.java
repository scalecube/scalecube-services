package io.scalecube.services.benchmarks.codecs;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageCodec;

import com.codahale.metrics.Timer;

import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.TimeUnit;

public class ServiceMessageWithByteBufDataEncodeBenchmarksRunner {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();
    new ServiceMessageCodecBenchmarkState(settings).runForSync(state -> {

      Timer timer = state.timer("timer");
      ServiceMessageCodec codec = state.codec();
      ServiceMessage message = state.messageWithByteBuf();

      return i -> {
        Timer.Context timeContext = timer.time();
        Object result = codec.encodeAndTransform(message, (dataByteBuf, headersByteBuf) -> {
          ReferenceCountUtil.release(headersByteBuf);
          return dataByteBuf;
        });
        timeContext.stop();
        return result;
      };
    });
  }
}
