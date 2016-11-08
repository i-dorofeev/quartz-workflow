package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.listeners.JobListenerSupport;

class EngineJobListener extends JobListenerSupport {

	private final TaskManager taskManager;

	EngineJobListener(TaskManager taskManager) {
		this.taskManager = taskManager;
	}

	@Override
	public String getName() {
		return "engineJobListener";
	}

	@Override
	public void jobToBeExecuted(JobExecutionContext context) {
		TaskId id = new TaskId(context.getTrigger().getKey().getName());
		taskManager.recordRunning(id);
	}

	@Override
	public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {

		TaskId id = new TaskId(context.getTrigger().getKey().getName());

		if (jobException == null)
			taskManager.recordSuccess(id);
		else
			taskManager.recordFailed(id, jobException);
	}

	private Task getTaskData(JobExecutionContext jeCtx) {
		TaskId id = new TaskId(jeCtx.getTrigger().getKey().getName());
		return this.taskManager.findTaskData(id).orElseThrow(() -> new EngineException("Couldn't find taskManager[id=" + id + "]"));
	}


}
