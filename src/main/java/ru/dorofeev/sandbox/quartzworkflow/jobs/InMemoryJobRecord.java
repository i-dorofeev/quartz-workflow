package ru.dorofeev.sandbox.quartzworkflow.jobs;

class InMemoryJobRecord {

	private String jobId;
	private String parentId;
	private String queueName;
	private String executionType;
	private String jobKey;
	private String serializedArgs;
	private String result;
	private String exception;

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
}
