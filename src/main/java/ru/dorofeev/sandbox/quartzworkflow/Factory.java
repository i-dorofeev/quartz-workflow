package ru.dorofeev.sandbox.quartzworkflow;

import ru.dorofeev.sandbox.quartzworkflow.engine.Engine;
import ru.dorofeev.sandbox.quartzworkflow.engine.EngineFactory;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorServiceFactory;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepositoryFactory;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStoreFactory;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManagerFactory;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStore;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import ru.dorofeev.sandbox.quartzworkflow.utils.Clock;
import ru.dorofeev.sandbox.quartzworkflow.utils.SystemClock;

public class Factory {

	public static Engine spawn(NodeId nodeId, SerializedObjectFactory serializedObjectFactory, JobStoreFactory jobStoreFactory, QueueStore queueStore, ExecutorServiceFactory executorServiceFactory) {
		Clock clock = new SystemClock();
		JobStore jobStore = jobStoreFactory.spawn(serializedObjectFactory);
		JobRepository jobRepository = JobRepositoryFactory.create(jobStore, clock);
		QueueManager queueManager = QueueManagerFactory.create(nodeId, queueStore);
		ExecutorService executorService = executorServiceFactory.spawn(nodeId);
		return EngineFactory.create(jobRepository, executorService, queueManager);
	}
}
