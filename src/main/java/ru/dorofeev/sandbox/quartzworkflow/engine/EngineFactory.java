package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;

public class EngineFactory {

	public static Engine create(JobRepository jobRepository, ExecutorService executorService, QueueManager queueManager) {
		return new EngineImpl(jobRepository, executorService, queueManager);
	}
}
