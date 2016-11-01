package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.listeners.JobListenerSupport;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

class EngineJobListener extends JobListenerSupport {

	private List<ExecutionInfo> failedExecutions = new CopyOnWriteArrayList<>();

	@Override
	public String getName() {
		return "engineJobListener";
	}

	@Override
	public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
		if (jobException != null)
			failedExecutions.add(
				new ExecutionInfo(context.getMergedJobDataMap(), context.getJobDetail().getKey(), new EngineException(jobException)));
	}

	List<ExecutionInfo> getFailedExecutions() {
		return new ArrayList<>(failedExecutions);
	}

	void resetFailedExecutions() {
		failedExecutions.clear();
	}

	void removeFailedExecution(ExecutionInfo executionInfo) {
		failedExecutions.remove(executionInfo);
	}
}
