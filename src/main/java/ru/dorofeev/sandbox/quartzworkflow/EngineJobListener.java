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
		TaskId id = new TaskId(context.getTrigger().getKey().getName());
		taskDataRepository.recordRunning(id);
	}

	@Override
	public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {

		TaskId id = new TaskId(context.getTrigger().getKey().getName());

		if (jobException == null)
			taskDataRepository.recordSuccess(id);
		else
			taskDataRepository.recordFailed(id, jobException);
	}

	private Task getTaskData(JobExecutionContext jeCtx) {
		TaskId id = new TaskId(jeCtx.getTrigger().getKey().getName());
		return this.taskDataRepository.findTaskData(id).orElseThrow(() -> new EngineException("Couldn't find taskDataRepository[id=" + id + "]"));
	}


}
