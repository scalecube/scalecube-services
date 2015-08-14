package io.servicefabric.cluster.gossip;

import com.google.common.base.Preconditions;
import io.protostuff.Tag;

/**
 * Data model for gossip, include gossip id, qualifier and object need to disseminate
 */
public final class Gossip {
	/** The id gossip. */
	@Tag(1)
	private String gossipId;
	/** The qualifier gossip data. */
	@Tag(2)
	private String qualifier;
	/** The data. */
	@Tag(3)
	private Object data;

	public Gossip() {
	}

	public Gossip(String gossipId, String qualifier, Object data) {
		Preconditions.checkArgument(gossipId != null);
		this.qualifier = qualifier;
		this.gossipId = gossipId;
		this.data = data;
	}

	public String getQualifier() {
		return qualifier;
	}

	void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}

	public String getGossipId() {
		return gossipId;
	}

	void setGossipId(String gossipId) {
		this.gossipId = gossipId;
	}

	public Object getData() {
		return data;
	}

	void setData(Object data) {
		this.data = data;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		Gossip gossip = (Gossip) o;

		if (!gossipId.equals(gossip.gossipId))
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		return gossipId.hashCode();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Gossip [qualifier=");
		builder.append(qualifier);
		builder.append(", gossipId=");
		builder.append(gossipId);
		builder.append(", data=");
		builder.append("***");
		builder.append("]");
		return builder.toString();
	}
}
