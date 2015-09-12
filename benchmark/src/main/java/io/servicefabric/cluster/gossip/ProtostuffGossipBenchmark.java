package io.servicefabric.cluster.gossip;

import io.servicefabric.transport.protocol.Message;
import io.servicefabric.transport.protocol.MessageDeserializer;
import io.servicefabric.transport.protocol.MessageSerializer;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.servicefabric.transport.protocol.ProtostuffProtocol;

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
          + "e03rXCPM1dhAGOgPjIFBFA2fQGTOlh8icxKVx8jwyFEqqehmIubcvfEFYmsb1QZgTgu2wdB4XxpTsspxUq4pc0O124AsU61W9tw7LZWlhij"
          + "1EA44sPrfjgW9kIZCEf8HL2zOXMUyNqRdzXD2VlxerrW7X8jcbYSCn5jkkXsc0E5Hf7P5DcuEcASrGH3ZhaGv94pnSneX9BJrRIQxtmAdPP"
          + "UXzVXrKw0mYcoB6Ye8mWuvVUFOO1Io6NJk0q2HHKST0FQVQo6mVEYKm8geTI6WphB4uRiQ6ksk6zOrJXmwQ6ssJIPYRRF2Mx9EDR9OMhYw6"
          + "hoijdfgd23EXz8WkTkYz42kQkK99rNxyIXVMVyRPzLHBclYaYKlcmoN8f7hq6aiv3VxlPPchZ6xWmjOlGJY9P7nINtChd2spMUkhAeznajS4VW";

  MessageSerializer ser;
  MessageDeserializer deser;

  Message gossipReq;
  ByteBuf gossipReqSer;
  ByteBuf bbGossipReq;

  Message gossipReqx32;
  ByteBuf gossipReqx32Ser;
  ByteBuf bbGossipReqx32;

  @Setup
  public void setup() {
    ProtostuffProtocol protocol = new ProtostuffProtocol();
    ser = protocol.getMessageSerializer();
    deser = protocol.getMessageDeserializer();

    gossipReq = new Message(new GossipRequest(ImmutableList.of(new Gossip("ABCDEFGH_0", new Message(PAYLOAD_X32)))));
    ser.serialize(gossipReq, gossipReqSer = Unpooled.buffer(1024));
    bbGossipReq = Unpooled.buffer(1024);
    System.err.println("### gossipReqSer=" + gossipReqSer);

    List<Gossip> list32 = new ArrayList<>();
    for (int i = 0; i < 32; i++) {
      list32.add(new Gossip("ABCDEFGH_" + i, new Message(PAYLOAD)));
    }
    gossipReqx32 = new Message(new GossipRequest(list32));
    ser.serialize(gossipReqx32, gossipReqx32Ser = Unpooled.buffer(1024));
    bbGossipReqx32 = Unpooled.buffer(1024);
    System.err.println("### gossipReqx32Ser=" + gossipReqx32Ser);
  }

  @Benchmark
  public void ser() {
    ser.serialize(gossipReq, bbGossipReq.resetWriterIndex());
  }

  @Benchmark
  public void deser() {
    deser.deserialize(gossipReqSer.resetReaderIndex());
  }

  @Benchmark
  public void ser_x32() {
    ser.serialize(gossipReqx32, bbGossipReqx32.resetWriterIndex());
  }

  @Benchmark
  public void deser_x32() {
    deser.deserialize(gossipReqx32Ser.resetReaderIndex());
  }
}
