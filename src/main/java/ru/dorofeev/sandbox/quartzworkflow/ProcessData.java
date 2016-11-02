package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.*;

import static org.quartz.TriggerBuilder.newTrigger;

public class ProcessData {

	public static final String PROCESS_DATA_ID = "processDataId";


	public enum Result {
		CREATED, RUNNING, SUCCESS, FAILED
	}

	private LocalId localId;
	private LocalId parentId;

	private final JobKey jobKey;
	private final JobDataMap jobData = new JobDataMap();
	private Result result = Result.CREATED;
	private Throwable exception;

	ProcessData(LocalId localId, LocalId parentId, JobKey jobKey, JobDataMap jobData) {
		this.localId = localId;
		this.parentId = parentId;
		this.jobKey = jobKey;
		this.jobData.putAll(jobData);
		this.jobData.put(PROCESS_DATA_ID, localId.toString());
	}

	public LocalId getLocalId() {
		return localId;
	}

	void setLocalId(LocalId localId) {
		this.localId = localId;
	}

	public JobDataMap getJobData() {
		return new JobDataMap(jobData);
	}

	public JobKey getJobKey() {
		return jobKey;
	}

	public LocalId getParentId() {
		return parentId;
	}

	public Result getResult() {
		return result;
	}

	public Throwable getException() {
		return exception;
	}

	public void recordRunning() {
		recordResult(Result.RUNNING, null);
	}

	public void recordSuccess() {
		recordResult(Result.SUCCESS, null);
	}

	public void recordFailed(Throwable ex) {
		recordResult(Result.FAILED, ex);
	}

	private void recordResult(Result res, Throwable ex) {
		result = res;
		exception = ex;
	}

	void enqueue(Scheduler scheduler) {
		Trigger trigger = newTrigger()
			.forJob(jobKey)
			.withIdentity(localId.toString())
			.usingJobData(new JobDataMap(jobData))
			.startNow()
			.build();

		scheduleTrigger(scheduler, trigger);
	}

	private void scheduleTrigger(Scheduler scheduler, Trigger trigger) {
		try {
			scheduler.scheduleJob(trigger);
		} catch (SchedulerException e) {
			throw new EngineException(e);
		}
	}

	public String prettyPrint() {
		return JsonUtils.toPrettyJson(this);
	}

	@Override
	public String toString() {
		return "ProcessData{" + localId + "}";
	}
}
