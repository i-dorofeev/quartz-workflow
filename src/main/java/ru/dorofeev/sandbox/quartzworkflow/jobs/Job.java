package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;

public class Job {

	private final JobId id;
	private final String queueName;
	private final QueueingOptions.ExecutionType executionType;
	private final Result result;
	private final String exception;
	private final JobKey jobKey;
	private final SerializedObject args;

	public Job(JobId id, String queueName, QueueingOptions.ExecutionType executionType, Result result, String exception, JobKey jobKey, SerializedObject args) {
		this.id = id;
		this.queueName = queueName;
		this.executionType = executionType;
		this.result = result;
		this.exception = exception;
		this.jobKey = jobKey;
		this.args = args;
	}

	public JobId getId() {
		return id;
	}

	public String getQueueName() {
		return queueName;
	}

	public QueueingOptions.ExecutionType getExecutionType() {
		return executionType;
	}

	public Job.Result getResult() {
		return result;
	}

	public String getException() {
		return exception;
	}

	public JobKey getJobKey() {
		return jobKey;
	}

	public SerializedObject getArgs() {
		return args;
	}

	@Override
	public String toString() {
		return "Job{" +
			"id=" + id +
			", queueName='" + queueName + '\'' +
			", executionType=" + executionType +
			", result=" + result +
			", exception='" + exception + '\'' +
			", jobKey=" + jobKey +
			", args=" + args +
			'}';
	}

	public enum Result {
		CREATED, RUNNING, SUCCESS, FAILED
	}
}
