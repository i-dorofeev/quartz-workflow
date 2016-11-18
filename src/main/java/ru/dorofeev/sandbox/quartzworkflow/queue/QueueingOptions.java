package ru.dorofeev.sandbox.quartzworkflow.queue;

public class QueueingOptions {

	public enum ExecutionType { EXCLUSIVE, PARALLEL }

	private final String queueName;
	private final ExecutionType executionType;

	public QueueingOptions(String queueName, ExecutionType executionType) {
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
