package io.servicefabric.cluster.gossip;

import java.util.List;

import io.protostuff.Tag;

/**
 * Gossip request which be transmitted through the network, contains list of gossips
 */
public final class GossipRequest {

	@Tag(1)
	private List<Gossip> gossipList;

	public GossipRequest(List<Gossip> gossipList) {
		this.gossipList = gossipList;
	}

	public List<Gossip> getGossipList() {
		return gossipList;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("GossipRequest{");
		sb.append("gossipList=").append(gossipList);
		sb.append('}');
		return sb.toString();
	}
}
