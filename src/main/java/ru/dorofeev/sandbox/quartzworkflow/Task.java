package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;
import org.quartz.JobKey;

public class Task {

	static final String TASK_ID = "taskId";


	public enum Result {
		CREATED, RUNNING, SUCCESS, FAILED
	}

	private final TaskId taskId;
	private final String queueName;

	private final JobKey jobKey;
	private final JobDataMap jobData = new JobDataMap();
	private Result result = Result.CREATED;
	private Throwable exception;

	Task(TaskId taskId, String queueName, JobKey jobKey, JobDataMap jobData) {
		this.taskId = taskId;
		this.queueName = queueName;
		this.jobKey = jobKey;
		this.jobData.putAll(jobData);
		this.jobData.put(TASK_ID, taskId.toString());
	}

	public TaskId getId() {
		return taskId;
	}

	Result getResult() {
		return result;
	}

	String getQueueName() {
		return queueName;
	}

	public Throwable getException() {
		return exception;
	}

	JobKey getJobKey() {
		return jobKey;
	}

	JobDataMap getJobData() {
		return jobData;
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