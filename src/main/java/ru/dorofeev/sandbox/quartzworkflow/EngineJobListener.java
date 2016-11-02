package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.listeners.JobListenerSupport;

class EngineJobListener extends JobListenerSupport {

	private final ProcessDataRepository processData;

	EngineJobListener(ProcessDataRepository processData) {
		this.processData = processData;
	}

	@Override
	public String getName() {
		return "engineJobListener";
	}

	@Override
	public void jobToBeExecuted(JobExecutionContext context) {
		ProcessData pd = getProcessData(context);
		pd.recordRunning();
	}

	@Override
	public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {

		ProcessData pd = getProcessData(context);

		if (jobException == null)
			pd.recordSuccess();
		else
			pd.recordFailed(jobException);
	}

	private ProcessData getProcessData(JobExecutionContext jeCtx) {
		GlobalId id = GlobalId.fromString(jeCtx.getTrigger().getKey().getName());
		return this.processData.findProcessData(id).orElseThrow(() -> new EngineException("Couldn't find processData[id=" + id + "]"));
	}
}
