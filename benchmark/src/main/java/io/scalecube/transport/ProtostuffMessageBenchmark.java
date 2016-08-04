package io.scalecube.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Fork(2)
@State(Scope.Thread)
@Threads(4)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ProtostuffMessageBenchmark {
  static final String PAYLOAD = "SgUKzpnrt8ArR9jz";
  static final String PAYLOAD_X16 = "M82qEG06ucgawpZ89PgJcBhWiDIOZgEgz8o42ZuBrXVEXlUNmXfPdY1BOh4UbbwxTuTNAeyosxlZjDOf"
      + "EfxPKPM2Al5CVkpg5175hzLBV5afcocm52JKwDvgSKVkoMzvnVWIQfjeAgGIERBgJ7a63mGygKDQS4moeHryedn68mmzNHGYbSqp7PIb6Rb"
      + "n8SgT1hSOATWBReLA4ZPqfGUV0miIgOU90EYXffu9aT4cc9V8rsz3q4W8ibMsxq1JMsB6";

  Message msg;
  ByteBuf msgSer;
  ByteBuf bbMsg;

  Message msgx16;
  ByteBuf msgx16Ser;
  ByteBuf bbMsgx16;

  /**
   * Setup benchmark.
   */
  @Setup
  public void setup() {

    msg = Message.fromData(PAYLOAD);
    MessageCodec.serialize(msg, msgSer = Unpooled.buffer(1024));
    bbMsg = Unpooled.buffer(1024);
    System.err.println("### msgSer=" + msgSer);

    msgx16 = Message.fromData(PAYLOAD_X16);
    MessageCodec.serialize(msgx16, msgx16Ser = Unpooled.buffer(1024));
    bbMsgx16 = Unpooled.buffer(1024);
    System.err.println("### msgx16Ser=" + msgx16Ser);
  }

  @Benchmark
  public void ser() {
    MessageCodec.serialize(msg, bbMsg.resetWriterIndex());
  }

  @Benchmark
  public void deser() {
    MessageCodec.deserialize(msgSer.resetReaderIndex());
  }

  @Benchmark
  public void ser_x16() {
    MessageCodec.serialize(msgx16, bbMsgx16.resetWriterIndex());
  }

  @Benchmark
  public void deser_x16() {
    MessageCodec.deserialize(msgx16Ser.resetReaderIndex());
  }
}
