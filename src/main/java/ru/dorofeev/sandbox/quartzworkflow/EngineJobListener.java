package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.listeners.JobListenerSupport;

class EngineJobListener extends JobListenerSupport {

	private final TaskRepository taskRepository;

	EngineJobListener(TaskRepository taskRepository) {
		this.taskRepository = taskRepository;
	}

	@Override
	public String getName() {
		return "engineJobListener";
	}

	@Override
	public void jobToBeExecuted(JobExecutionContext context) {
		TaskId id = new TaskId(context.getTrigger().getKey().getName());
		taskRepository.recordRunning(id);
	}

	@Override
	public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {

		TaskId id = new TaskId(context.getTrigger().getKey().getName());

		if (jobException == null)
			taskRepository.recordSuccess(id);
		else
			taskRepository.recordFailed(id, jobException);
	}
}
