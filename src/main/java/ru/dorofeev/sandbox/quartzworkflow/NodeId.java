package ru.dorofeev.sandbox.quartzworkflow;

import static ru.dorofeev.sandbox.quartzworkflow.utils.Contracts.shouldNotBeEmpty;

public class NodeId {

	public static NodeId fromString(String nodeId) {
		if (nodeId == null)
			return null;
		else
			return new NodeId(nodeId);
	}

	private final String id;

	public NodeId(String id) {
		shouldNotBeEmpty(id, "Node id should not be empty or null string.");

		this.id = id;
	}

	public String value() {
		return id;
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
