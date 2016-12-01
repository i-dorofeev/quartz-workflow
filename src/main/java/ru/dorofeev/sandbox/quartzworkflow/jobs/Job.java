package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;

import java.util.Date;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public class Job {

	private final JobId id;
	private final JobId parentId;
	private final String queueName;
	private final QueueingOptions.ExecutionType executionType;
	private final Result result;
	private final String exception;
	private final JobKey jobKey;
	private final SerializedObject args;
	private final Date created;
	private final Long executionDuration;
	private final Date completed;

	public Job(JobId id, JobId parentId, String queueName, QueueingOptions.ExecutionType executionType, Result result, String exception, JobKey jobKey, SerializedObject args, Date created, Long executionDuration, Date completed) {
		this.id = id;
		this.parentId = parentId;
		this.queueName = queueName;
		this.executionType = executionType;
		this.result = result;
		this.exception = exception;
		this.jobKey = jobKey;
		this.args = args;
		this.created = created;
		this.executionDuration = executionDuration;
		this.completed = completed;
	}

	public JobId getId() {
		return id;
	}

	public Optional<JobId> getParentId() {
		return ofNullable(parentId);
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

	public Optional<String> getException() {
		return ofNullable(exception);
	}

	public JobKey getJobKey() {
		return jobKey;
	}

	public SerializedObject getArgs() {
		return args;
	}

	public Date getCreated() {
		return created;
	}

	public Long getExecutionDuration() {
		return executionDuration;
	}

	public Date getCompleted() {
		return completed;
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
		CREATED, SUCCESS, FAILED
	}
}
