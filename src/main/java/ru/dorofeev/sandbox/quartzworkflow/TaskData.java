package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;
import org.quartz.JobKey;

public class TaskData {

	static final String TASK_DATA_ID = "taskDataId";


	public enum Result {
		CREATED, RUNNING, SUCCESS, FAILED
	}

	private final TaskId taskId;
	private final String queueName;

	private final JobKey jobKey;
	private final JobDataMap jobData = new JobDataMap();
	private Result result = Result.CREATED;
	private Throwable exception;

	TaskData(TaskId taskId, String queueName, JobKey jobKey, JobDataMap jobData) {
		this.taskId = taskId;
		this.queueName = queueName;
		this.jobKey = jobKey;
		this.jobData.putAll(jobData);
		this.jobData.put(TASK_DATA_ID, taskId.toString());
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

	public Throwable getException() {
		return exception;
	}

	JobKey getJobKey() {
		return jobKey;
	}

	JobDataMap getJobData() {
		return jobData;
	}

	void recordResult(TaskData.Result res, Throwable ex) {
		result = res;
		exception = ex;
	}

	public String prettyPrint() {
		return JsonUtils.toPrettyJson(this);
	}

	@Override
	public String toString() {
		return "TaskData{" + taskId + "}";
	}
}
