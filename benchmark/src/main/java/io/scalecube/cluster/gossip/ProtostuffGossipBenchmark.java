package io.scalecube.cluster.gossip;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageCodec;

import com.google.common.collect.ImmutableList;

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Fork(2)
@State(Scope.Thread)
@Threads(4)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ProtostuffGossipBenchmark {
  static final String PAYLOAD = "Tl4KqQXZ5aMiIw29";
  static final String PAYLOAD_X32 =
      "tI8Ppp8ShAp7IEDSFV1IgZCDKH2WyLI0NeSNc9oQOhQZXcHOzktuQBQmT5EGNitohtS1LShvdHgtAtWRz"
          + "e03rXCPM1dhAGOgPjIFBFA2fQGTOlh8icxKVx8jwyFEqqehmIubcvfEFYmsb1QZgTgu2wdB4XxpTsspxUq4pc0O124AsU61W9tw7L"
          + "ZWlhij1EA44sPrfjgW9kIZCEf8HL2zOXMUyNqRdzXD2VlxerrW7X8jcbYSCn5jkkXsc0E5Hf7P5DcuEcASrGH3ZhaGv94pnSneX9BJ"
          + "rRIQxtmAdPPUXzVXrKw0mYcoB6Ye8mWuvVUFOO1Io6NJk0q2HHKST0FQVQo6mVEYKm8geTI6WphB4uRiQ6ksk6zOrJXmwQ6ssJIPYR"
          + "RF2Mx9EDR9OMhYw6hoijdfgd23EXz8WkTkYz42kQkK99rNxyIXVMVyRPzLHBclYaYKlcmoN8f7hq6aiv3VxlPPchZ6xWmjOlGJY9P7"
          + "nINtChd2spMUkhAeznajS4VW";

  Message gossipReq;
  ByteBuf gossipReqSer;
  ByteBuf bbGossipReq;

  Message gossipReqx32;
  ByteBuf gossipReqx32Ser;
  ByteBuf bbGossipReqx32;

  /**
   * Setup benchmark.
   */
  @Setup
  public void setup() {
    List<Gossip> gossips = ImmutableList.of(new Gossip("ABCDEFGH_0", Message.fromData(PAYLOAD_X32)));
    Member from = new Member("0", Address.from("localhost:1234"));
    gossipReq = Message.fromData(new GossipRequest(gossips, from.id()));
    MessageCodec.serialize(gossipReq, gossipReqSer = Unpooled.buffer(1024));
    bbGossipReq = Unpooled.buffer(1024);
    System.err.println("### gossipReqSer=" + gossipReqSer);

    List<Gossip> list32 = new ArrayList<>();
    for (int i = 0; i < 32; i++) {
      list32.add(new Gossip("ABCDEFGH_" + i, Message.fromData(PAYLOAD)));
    }
    gossipReqx32 = Message.fromData(new GossipRequest(list32, from.id()));
    MessageCodec.serialize(gossipReqx32, gossipReqx32Ser = Unpooled.buffer(1024));
    bbGossipReqx32 = Unpooled.buffer(1024);
    System.err.println("### gossipReqx32Ser=" + gossipReqx32Ser);
  }

  @Benchmark
  public void ser() {
    MessageCodec.serialize(gossipReq, bbGossipReq.resetWriterIndex());
  }

  @Benchmark
  public void deser() {
    MessageCodec.deserialize(gossipReqSer.resetReaderIndex());
  }

  @Benchmark
  public void ser_x32() {
    MessageCodec.serialize(gossipReqx32, bbGossipReqx32.resetWriterIndex());
  }

  @Benchmark
  public void deser_x32() {
    MessageCodec.deserialize(gossipReqx32Ser.resetReaderIndex());
  }
}
