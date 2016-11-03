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
		TaskData td = getTaskData(context);
		td.recordRunning();
	}

	@Override
	public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {

		TaskData td = getTaskData(context);

		if (jobException == null)
			td.recordSuccess();
		else
			td.recordFailed(jobException);
	}

	private TaskData getTaskData(JobExecutionContext jeCtx) {
		TaskId id = new TaskId(jeCtx.getTrigger().getKey().getName());
		return this.taskDataRepository.findTaskData(id).orElseThrow(() -> new EngineException("Couldn't find taskDataRepository[id=" + id + "]"));
	}
}
