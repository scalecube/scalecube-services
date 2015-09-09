package io.servicefabric.transport.protocol;

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

  ProtostuffMessageSerializer ser;
  ProtostuffMessageDeserializer deser;

  Message msg;
  ByteBuf msg_ser;
  ByteBuf bb_msg;

  Message msg_x16;
  ByteBuf msg_x16_ser;
  ByteBuf bb_msg_x16;

  @Setup
  public void setup() {
    ser = new ProtostuffMessageSerializer();
    deser = new ProtostuffMessageDeserializer();

    msg = new Message(PAYLOAD);
    ser.serialize(msg, msg_ser = Unpooled.buffer(1024));
    bb_msg = Unpooled.buffer(1024);
    System.err.println("### msg_ser=" + msg_ser);

    msg_x16 = new Message(PAYLOAD_X16);
    ser.serialize(msg_x16, msg_x16_ser = Unpooled.buffer(1024));
    bb_msg_x16 = Unpooled.buffer(1024);
    System.err.println("### msg_x16_ser=" + msg_x16_ser);
  }

  @Benchmark
  public void ser() {
    ser.serialize(msg, bb_msg.resetWriterIndex());
  }

  @Benchmark
  public void deser() {
    deser.deserialize(msg_ser.resetReaderIndex());
  }

  @Benchmark
  public void ser_x16() {
    ser.serialize(msg_x16, bb_msg_x16.resetWriterIndex());
  }

  @Benchmark
  public void deser_x16() {
    deser.deserialize(msg_x16_ser.resetReaderIndex());
  }
}
