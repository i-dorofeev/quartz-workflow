package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;
import org.quartz.JobKey;

public class TaskData {

	static final String TASK_DATA_ID = "taskDataId";


	public enum Result {
		CREATED, RUNNING, SUCCESS, FAILED
	}

	private TaskId taskId;

	private final JobKey jobKey;
	private final JobDataMap jobData = new JobDataMap();
	private Result result = Result.CREATED;
	private Throwable exception;

	TaskData(TaskId taskId, JobKey jobKey, JobDataMap jobData) {
		this.taskId = taskId;
		this.jobKey = jobKey;
		this.jobData.putAll(jobData);
		this.jobData.put(TASK_DATA_ID, taskId.toString());
	}

	public TaskId getTaskId() {
		return taskId;
	}

	public Result getResult() {
		return result;
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

	void recordRunning() {
		recordResult(Result.RUNNING, null);
	}

	void recordSuccess() {
		recordResult(Result.SUCCESS, null);
	}

	void recordFailed(Throwable ex) {
		recordResult(Result.FAILED, ex);
	}

	private void recordResult(Result res, Throwable ex) {
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
