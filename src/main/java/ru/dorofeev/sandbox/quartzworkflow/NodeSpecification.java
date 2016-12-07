package ru.dorofeev.sandbox.quartzworkflow;

import static ru.dorofeev.sandbox.quartzworkflow.utils.Contracts.shouldNotBeEmpty;
import static ru.dorofeev.sandbox.quartzworkflow.utils.Contracts.shouldNotBeNull;

public class NodeSpecification {

	public static final NodeSpecification ANY_NODE = new NodeSpecification();
	public static final String ANY_NODE_STRING_SPECIFICATION = "*";

	public static NodeSpecification fromString(String nodeSpecification) {
		shouldNotBeEmpty(nodeSpecification, "Node specification should not be null.");

		String trimmedNodeSpecification = nodeSpecification.trim();

		if (ANY_NODE_STRING_SPECIFICATION.equals(trimmedNodeSpecification))
			return ANY_NODE;
		else
			return new NodeSpecification(new NodeId(trimmedNodeSpecification));
	}

	private final NodeId nodeId;

	public NodeSpecification(NodeId nodeId) {
		shouldNotBeNull(nodeId, "Invalid node specification. Non null nodeId should be specified. Use ANY_NODE constant to match any node.");

		this.nodeId = nodeId;
	}

	private NodeSpecification() {
		this.nodeId = null;
	}

	public NodeId getNodeId() {
		return nodeId;
	}

	public boolean matches(NodeId nodeId) {

		shouldNotBeNull(nodeId, "Cannot match node spectification to null node id. Node id should be specified.");

		return this.equals(ANY_NODE) || this.nodeId.equals(nodeId);
	}

	public String asString() {
		if (this.nodeId == null)
			return ANY_NODE_STRING_SPECIFICATION;
		else
			return nodeId.value();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		NodeSpecification that = (NodeSpecification) o;

		return nodeId != null ? nodeId.equals(that.nodeId) : that.nodeId == null;
	}

	@Override
	public int hashCode() {
		return nodeId != null ? nodeId.hashCode() : 0;
	}

	@Override
	public String toString() {
		return "NodeSpecification{" +
			"nodeId=" + nodeId +
			'}';
	}
}
