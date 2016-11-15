package ru.dorofeev.sandbox.quartzworkflow.queue;

public class QueueingOption {

	public enum ExecutionType { EXCLUSIVE, PARALLEL }

	private final String queueName;
	private final ExecutionType executionType;

	public QueueingOption(String queueName, ExecutionType executionType) {
		this.queueName = queueName;
		this.executionType = executionType;
	}

	public String getQueueName() {
		return queueName;
	}

	public ExecutionType getExecutionType() {
		return executionType;
	}
}
