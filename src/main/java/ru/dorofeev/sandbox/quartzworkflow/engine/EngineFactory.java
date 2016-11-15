package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository;

public class EngineFactory {

	public static Engine create(JobRepository jobRepository, ExecutorService executorService, QueueManager queueManager, SerializedObjectFactory serializedObjectFactory) {
		return new EngineImpl(jobRepository, executorService, queueManager, serializedObjectFactory);
	}
}
