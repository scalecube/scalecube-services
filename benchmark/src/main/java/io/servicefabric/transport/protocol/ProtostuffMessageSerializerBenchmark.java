package io.servicefabric.transport.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.servicefabric.cluster.gossip.Gossip;
import io.servicefabric.cluster.gossip.GossipRequest;
import io.servicefabric.transport.TransportHeaders;

@Fork(2)
@State(Scope.Thread)
@Threads(4)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ProtostuffMessageSerializerBenchmark {

	static final byte[] GENERIC_DATA = new byte[128];
	static {
		ThreadLocalRandom.current().nextBytes(GENERIC_DATA);
	}

	static final byte[] SMALL_DATA = new byte[64];
	static {
		ThreadLocalRandom.current().nextBytes(SMALL_DATA);
	}

	static final byte[] DATA_1K = new byte[1024];
	static {
		ThreadLocalRandom.current().nextBytes(DATA_1K);
	}

	static final Map<String, String> GENERIC_HEADERS = ImmutableMap.of(
			TransportHeaders.QUALIFIER, "q",
			TransportHeaders.CORRELATION_ID, "cid"
			);

	ProtostuffMessageSerializer ser;
	ProtostuffMessageDeserializer deser;

	Message msg;
	ByteBuf bb_msg_ser;
	ByteBuf bb_msg;

	Message msg1k;
	ByteBuf bb_msg1k_ser;
	ByteBuf bb_msg1k;

	Message gossipReq;
	ByteBuf bb_gossipReq_ser;
	ByteBuf bb_gossipReq;

	Message gossipReqSmallData;
	ByteBuf bb_gossipReqSmallData_ser;
	ByteBuf bb_gossipReqSmallData;

	@Setup
	public void setup() {
		ser = new ProtostuffMessageSerializer();
		deser = new ProtostuffMessageDeserializer();

		msg = new Message(GENERIC_DATA, GENERIC_HEADERS);
		ser.serialize(msg, bb_msg_ser = Unpooled.buffer());
		bb_msg = Unpooled.buffer(GENERIC_DATA.length);

		msg1k = new Message(DATA_1K, GENERIC_HEADERS);
		ser.serialize(msg1k, bb_msg1k_ser = Unpooled.buffer());
		bb_msg1k = Unpooled.buffer(DATA_1K.length);

		List<Gossip> list10 = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			list10.add(new Gossip("ABCDEFGH_" + i, new Message(GENERIC_DATA, GENERIC_HEADERS)));
		}
		gossipReq = new Message(new GossipRequest(list10));
		ser.serialize(gossipReq, bb_gossipReq_ser = Unpooled.buffer());
		bb_gossipReq = Unpooled.buffer(1024);

		List<Gossip> list100 = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			list100.add(new Gossip("ABCDEFGH_" + i, new Message(SMALL_DATA, GENERIC_HEADERS)));
		}
		gossipReqSmallData = new Message(new GossipRequest(list100));
		ser.serialize(gossipReqSmallData, bb_gossipReqSmallData_ser = Unpooled.buffer());
		bb_gossipReqSmallData = Unpooled.buffer(1024);
	}

	@Benchmark
	public void ser() {
		ser.serialize(msg, bb_msg.resetWriterIndex());
	}

	@Benchmark
	public void deser() {
		deser.deserialize(bb_msg_ser.resetReaderIndex());
	}

	@Benchmark
	public void ser1k() {
		ser.serialize(msg1k, bb_msg1k.resetWriterIndex());
	}

	@Benchmark
	public void deser1k() {
		deser.deserialize(bb_msg1k_ser.resetReaderIndex());
	}

	@Benchmark
	public void serGossipReq() {
		ser.serialize(gossipReq, bb_gossipReq.resetWriterIndex());
	}

	@Benchmark
	public void deserGossipReq() {
		deser.deserialize(bb_gossipReq_ser.resetReaderIndex());
	}

	@Benchmark
	public void serGossipReqSmallData() {
		ser.serialize(gossipReqSmallData, bb_gossipReqSmallData.resetWriterIndex());
	}

	@Benchmark
	public void deserGossipReqSmallData() {
		deser.deserialize(bb_gossipReqSmallData_ser.resetReaderIndex());
	}
}
