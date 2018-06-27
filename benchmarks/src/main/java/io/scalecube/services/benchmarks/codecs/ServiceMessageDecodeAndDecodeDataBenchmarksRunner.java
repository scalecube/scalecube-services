package io.scalecube.services.benchmarks.codecs;

import io.scalecube.benchmarks.BenchmarksSettings;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codec.ServiceMessageCodec;

import com.codahale.metrics.Timer;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;

import java.util.concurrent.TimeUnit;

public class ServiceMessageDecodeAndDecodeDataBenchmarksRunner {

  public static void main(String[] args) {
    BenchmarksSettings settings = BenchmarksSettings.from(args).durationUnit(TimeUnit.NANOSECONDS).build();
    new ServiceMessageCodecBenchmarkState(settings).runForSync(state -> {

      Timer timer = state.timer("timer");
      ServiceMessageCodec codec = state.codec();
      Payload payloadMessage = state.payload();
      Class<?> dataType = state.dataType();

      return i -> {
        Timer.Context timeContext = timer.time();
        ByteBuf dataBuffer = payloadMessage.sliceData().retain();
        ByteBuf headersBuffer = payloadMessage.sliceMetadata().retain();
        ServiceMessage message = ServiceMessageCodec.decodeData(codec.decode(dataBuffer, headersBuffer), dataType);
        timeContext.stop();
        return message;
      };
    });
  }
}
