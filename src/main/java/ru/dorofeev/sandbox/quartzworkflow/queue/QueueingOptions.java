package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.NodeSpecification;
import ru.dorofeev.sandbox.quartzworkflow.utils.entrypoint.API;

import static ru.dorofeev.sandbox.quartzworkflow.utils.Contracts.shouldNotBeEmpty;
import static ru.dorofeev.sandbox.quartzworkflow.utils.Contracts.shouldNotBeNull;

public class QueueingOptions {

	@API public static final String DEFAULT_QUEUE_NAME = "default";
	@API public static final ExecutionType DEFAULT_EXECUTION_TYPE = ExecutionType.PARALLEL;
	@API public static final NodeSpecification DEFAULT_NODE_SPECIFICATION = NodeSpecification.ANY_NODE;

	@API public static final QueueingOptions DEFAULT = new QueueingOptions(DEFAULT_QUEUE_NAME, DEFAULT_EXECUTION_TYPE, DEFAULT_NODE_SPECIFICATION);

	public enum ExecutionType { EXCLUSIVE, PARALLEL }

	private final String queueName;
	private final ExecutionType executionType;
	private final NodeSpecification targetNodeSpecification;

	public QueueingOptions(String queueName, ExecutionType executionType, NodeSpecification targetNodeSpecification) {

		shouldNotBeEmpty(queueName, "Queue name should not be empty. You may use QueueingOptions.DEFAULT_QUEUE_NAME instead of null or empty string.");
		shouldNotBeNull(executionType, "Execution type should be specified explicitly. You may use QueueingOptions.DEFAULT_EXECUTION_TYPE constant instead of null.");
		shouldNotBeNull(targetNodeSpecification, "Target node specification should be specified explicitly. You may use QueueingOptions.DEFAULT_NODE_SPECIFICATION constant instead of null.");

		this.queueName = queueName;
		this.executionType = executionType;
		this.targetNodeSpecification = targetNodeSpecification;
	}

	public String getQueueName() {
		return queueName;
	}

	public ExecutionType getExecutionType() {
		return executionType;
	}

	public NodeSpecification getTargetNodeSpecification() {
		return targetNodeSpecification;
	}
}
