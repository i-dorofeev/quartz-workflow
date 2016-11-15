package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import ru.dorofeev.sandbox.quartzworkflow.utils.JsonUtils;

public class Job {

	public enum Result {
		CREATED, RUNNING, SUCCESS, FAILED
	}

	private final JobId jobId;
	private final String queueName;
	private final QueueingOption.ExecutionType executionType;

	private final JobKey jobKey;
	private final SerializedObject args;
	private Result result = Result.CREATED;
	private Throwable exception;

	Job(JobId jobId, String queueName, QueueingOption.ExecutionType executionType, JobKey jobKey, SerializedObject args) {
		this.jobId = jobId;
		this.queueName = queueName;
		this.executionType = executionType;
		this.jobKey = jobKey;
		this.args = args;
	}

	public JobId getId() {
		return jobId;
	}

	Result getResult() {
		return result;
	}

	public String getQueueName() {
		return queueName;
	}

	public QueueingOption.ExecutionType getExecutionType() {
		return executionType;
	}

	public Throwable getException() {
		return exception;
	}

	public JobKey getJobKey() {
		return jobKey;
	}

	public SerializedObject getArgs() {
		return args;
	}

	void recordResult(Job.Result res, Throwable ex) {
		result = res;
		exception = ex;
	}

	public String prettyPrint() {
		return JsonUtils.toPrettyJson(this);
	}

	@Override
	public String toString() {
		return "Job{" + jobId + "}";
	}
}
