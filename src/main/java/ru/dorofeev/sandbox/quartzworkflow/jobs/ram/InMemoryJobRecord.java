package ru.dorofeev.sandbox.quartzworkflow.jobs.ram;

import java.util.Date;

class InMemoryJobRecord {

	private String jobId;
	private String parentId;
	private String queueName;
	private String executionType;
	private String jobKey;
	private String serializedArgs;
	private String result;
	private String exception;
	private Date created;
	private String targetNodeSpecification;
	private Long executionDuration;
	private Date completed;
	private String completedNodeId;

	String getJobId() {
		return jobId;
	}

	void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getParentId() {
		return parentId;
	}

	public void setParentId(String parentId) {
		this.parentId = parentId;
	}

	String getQueueName() {
		return queueName;
	}

	void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	String getExecutionType() {
		return executionType;
	}

	void setExecutionType(String executionType) {
		this.executionType = executionType;
	}

	String getJobKey() {
		return jobKey;
	}

	void setJobKey(String jobKey) {
		this.jobKey = jobKey;
	}

	String getSerializedArgs() {
		return serializedArgs;
	}

	void setSerializedArgs(String serializedArgs) {
		this.serializedArgs = serializedArgs;
	}

	String getResult() {
		return result;
	}

	void setResult(String result) {
		this.result = result;
	}

	String getException() {
		return exception;
	}

	void setException(String exception) {
		this.exception = exception;
	}

	Date getCreated() {
		return created;
	}

	void setCreated(Date created) {
		this.created = created;
	}

	String getTargetNodeSpecification() {
		return targetNodeSpecification;
	}

	void setTargetNodeSpecification(String targetNodeSpecification) {
		this.targetNodeSpecification = targetNodeSpecification;
	}

	public Long getExecutionDuration() {
		return executionDuration;
	}

	public void setExecutionDuration(Long executionDuration) {
		this.executionDuration = executionDuration;
	}

	public Date getCompleted() {
		return completed;
	}

	public void setCompleted(Date completed) {
		this.completed = completed;
	}

	public String getCompletedNodeId() {
		return completedNodeId;
	}

	public void setCompletedNodeId(String completedNodeId) {
		this.completedNodeId = completedNodeId;
	}
}
