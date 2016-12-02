package ru.dorofeev.sandbox.quartzworkflow;

import org.springframework.util.Assert;

public class NodeId {

	public static final NodeId ANY_NODE = new NodeId();

	private final String id;

	public NodeId(String id) {
		Assert.notNull(id, "id mustn't be null. Use ANY_NODE constant instead.");
		this.id = id;
	}

	private NodeId() {
		this.id = null;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		NodeId nodeId = (NodeId) o;

		return id != null ? id.equals(nodeId.id) : nodeId.id == null;
	}

	@Override
	public int hashCode() {
		return id != null ? id.hashCode() : 0;
	}

	@Override
	public String toString() {
		return "NodeId{" +
			"id='" + id + '\'' +
			'}';
	}
}
