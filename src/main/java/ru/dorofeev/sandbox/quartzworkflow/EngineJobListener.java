package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.listeners.JobListenerSupport;

class EngineJobListener extends JobListenerSupport {

	private final TaskDataRepository taskDataRepository;

	EngineJobListener(TaskDataRepository taskDataRepository) {
		this.taskDataRepository = taskDataRepository;
	}

	@Override
	public String getName() {
		return "engineJobListener";
	}

	@Override
	public void jobToBeExecuted(JobExecutionContext context) {
		TaskData pd = getTaskData(context);
		pd.recordRunning();
	}

	@Override
	public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {

		TaskData pd = getTaskData(context);

		if (jobException == null)
			pd.recordSuccess();
		else
			pd.recordFailed(jobException);
	}

	private TaskData getTaskData(JobExecutionContext jeCtx) {
		TaskId id = new TaskId(jeCtx.getTrigger().getKey().getName());
		return this.taskDataRepository.findTaskData(id).orElseThrow(() -> new EngineException("Couldn't find taskDataRepository[id=" + id + "]"));
	}
}
