package ru.dorofeev.sandbox.quartzworkflow.taskrepo;

import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import ru.dorofeev.sandbox.quartzworkflow.utils.JsonUtils;

public class Task {

	public enum Result {
		CREATED, RUNNING, SUCCESS, FAILED
	}

	private final TaskId taskId;
	private final String queueName;
	private final QueueingOption.ExecutionType executionType;

	private final JobKey jobKey;
	private final SerializedObject args;
	private Result result = Result.CREATED;
	private Throwable exception;

	Task(TaskId taskId, String queueName, QueueingOption.ExecutionType executionType, JobKey jobKey, SerializedObject args) {
		this.taskId = taskId;
		this.queueName = queueName;
		this.executionType = executionType;
		this.jobKey = jobKey;
		this.args = args;
	}

	public TaskId getId() {
		return taskId;
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

	void recordResult(Task.Result res, Throwable ex) {
		result = res;
		exception = ex;
	}

	public String prettyPrint() {
		return JsonUtils.toPrettyJson(this);
	}

	@Override
	public String toString() {
		return "Task{" + taskId + "}";
	}
}
