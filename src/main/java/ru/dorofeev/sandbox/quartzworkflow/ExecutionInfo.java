package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobDataMap;
import org.quartz.JobKey;

public class ExecutionInfo {

	private final JobDataMap jobData;
	private final JobKey jobKey;
	private final EngineException exception;

	ExecutionInfo(JobDataMap jobData, JobKey jobKey, EngineException exception) {
		this.jobData = jobData;
		this.jobKey = jobKey;
		this.exception = exception;
	}

	JobKey getJobKey() {
		return jobKey;
	}

	JobDataMap getJobData() {
		return new JobDataMap(jobData);
	}

	public EngineException getException() {
		return exception;
	}

	@Override
	public String toString() {
		return "ExecutionInfo {" +
			"jobData=" + jobData.getWrappedMap() +
			", jobKey=" + jobKey +
			", exception=" + exception +
			'}';
	}

}
