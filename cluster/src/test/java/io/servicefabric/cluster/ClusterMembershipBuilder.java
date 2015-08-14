package io.servicefabric.cluster;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.SettableFuture;
import io.servicefabric.transport.*;
import rx.schedulers.Schedulers;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import io.servicefabric.cluster.fdetector.FailureDetectorBuilder;
import io.servicefabric.cluster.gossip.GossipProtocol;

public class ClusterMembershipBuilder {
	final ClusterMembership target;
	final GossipProtocol gossipProtocol;
	final FailureDetectorBuilder fdBuilder;
	final Transport transport;

	private ClusterMembershipBuilder(ClusterEndpoint clusterEndpoint, List<TransportEndpoint> members) {
		transport = (Transport) TransportBuilder.newInstance(clusterEndpoint.endpoint(), clusterEndpoint.endpointId())
				.useNetworkEmulator()
				.build();

		fdBuilder = FailureDetectorBuilder.FDBuilder(clusterEndpoint, transport).pingTime(100).pingTimeout(100);

		gossipProtocol = new GossipProtocol(clusterEndpoint, Executors.newSingleThreadScheduledExecutor());
		gossipProtocol.setTransport(transport);

		target = new ClusterMembership(clusterEndpoint, Schedulers.computation());
		target.setTransport(transport);
		target.setFailureDetector(fdBuilder.target());
		target.setGossipProtocol(gossipProtocol);
		target.setLocalMetadata(new HashMap<String, String>() {
			{
				put("key", "val");
			}
		});
		target.setWellknownMemberList(members);
		target.setSyncTime(1000);
		target.setSyncTimeout(100);
	}

	public static ClusterMembershipBuilder CMBuilder(ClusterEndpoint clusterEndpoint, List<TransportEndpoint> members) {
		return new ClusterMembershipBuilder(clusterEndpoint, members);
	}

	public static ClusterMembershipBuilder CMBuilder(ClusterEndpoint clusterEndpoint, TransportEndpoint... members) {
		return new ClusterMembershipBuilder(clusterEndpoint, Arrays.asList(members));
	}

	public ClusterMembershipBuilder syncTime(int syncTime) {
		target.setSyncTime(syncTime);
		return this;
	}

	public ClusterMembershipBuilder syncTimeout(int syncTimeout) {
		target.setSyncTimeout(syncTimeout);
		return this;
	}

	public ClusterMembershipBuilder maxSuspectTime(int maxSuspectTime) {
		target.setMaxSuspectTime(maxSuspectTime);
		return this;
	}

	public ClusterMembershipBuilder maxShutdownTime(int maxShutdownTime) {
		target.setMaxShutdownTime(maxShutdownTime);
		return this;
	}

	public ClusterMembershipBuilder pingTime(int pingTime) {
		fdBuilder.pingTime(pingTime);
		return this;
	}

	public ClusterMembershipBuilder pingTimeout(int pingTimeout) {
		fdBuilder.pingTimeout(pingTimeout);
		return this;
	}

	public ClusterMembershipBuilder ping(ClusterEndpoint member) {
		fdBuilder.ping(member);
		return this;
	}

	public ClusterMembershipBuilder noRandomMembers() {
		fdBuilder.noRandomMembers();
		return this;
	}

	public ClusterMembershipBuilder randomMembers(List<ClusterEndpoint> members) {
		fdBuilder.randomMembers(members);
		return this;
	}

	public ClusterMembershipBuilder block(ClusterEndpoint dest) {
		transport.<SocketChannelPipelineFactory> getPipelineFactory().blockMessagesTo(dest.endpoint());
		return this;
	}

	public ClusterMembershipBuilder block(List<ClusterEndpoint> members) {
		for (ClusterEndpoint dest : members) {
			block(dest);
		}
		return this;
	}

	public ClusterMembershipBuilder network(ClusterEndpoint member, int lostPercent, int mean) {
		transport.<SocketChannelPipelineFactory> getPipelineFactory().setNetworkSettings(member.endpoint(), lostPercent, mean);
		return this;
	}

	public ClusterMembershipBuilder unblock(TransportEndpoint dest) {
		transport.<SocketChannelPipelineFactory> getPipelineFactory().unblockMessagesTo(dest);
		return this;
	}

	public ClusterMembershipBuilder unblock(List<TransportEndpoint> members) {
		for (TransportEndpoint dest : members) {
			unblock(dest);
		}
		return this;
	}

	public ClusterMembershipBuilder unblockAll() {
		transport.<SocketChannelPipelineFactory> getPipelineFactory().unblockAll();
		return this;
	}

	ClusterMembershipBuilder init() {
		transport.start();
		fdBuilder.target().start();
		gossipProtocol.start();
		target.start();
		return this;
	}

	void destroy() {
		destroyTransport(transport);
		fdBuilder.target().stop();
		gossipProtocol.stop();
		target.stop();
	}

	private void destroyTransport(ITransport tf) {
		SettableFuture<Void> close = SettableFuture.create();
		tf.stop(close);
		try {
			close.get(1, TimeUnit.SECONDS);
		} catch (Exception ignore) {
		}
		try {
			Thread.sleep(10);
		} catch (InterruptedException ignore) {
		}
	}

	public ClusterMembershipBuilder assertTrusted(ClusterEndpoint... members) {
		assertStatus(ClusterMemberStatus.TRUSTED, members);
		return this;
	}

	public ClusterMembershipBuilder assertSuspected(ClusterEndpoint... members) {
		assertStatus(ClusterMemberStatus.SUSPECTED, members);
		return this;
	}

	public ClusterMembershipBuilder assertNoSuspected() {
		assertStatus(ClusterMemberStatus.SUSPECTED, new ClusterEndpoint[0]);
		return this;
	}

	private void assertStatus(final ClusterMemberStatus s, ClusterEndpoint[] members) {
		Predicate<ClusterMember> predicate = new Predicate<ClusterMember>() {
			@Override
			public boolean apply(@Nullable ClusterMember input) {
				return input.status() == s;
			}
		};
		Function<ClusterMember, ClusterEndpoint> function = new Function<ClusterMember, ClusterEndpoint>() {
			@Override
			public ClusterEndpoint apply(ClusterMember input) {
				return input.endpoint();
			}
		};
		List<ClusterEndpoint> list = newArrayList(transform(filter(target.members(), predicate), function));
		assertEquals("expect " + s + ": " + list, members.length, list.size());
		for (ClusterEndpoint member : members) {
			assertTrue("expect " + s + ": " + member, list.contains(member));
		}
	}
}
