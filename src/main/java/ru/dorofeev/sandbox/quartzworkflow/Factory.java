package ru.dorofeev.sandbox.quartzworkflow;

import ru.dorofeev.sandbox.quartzworkflow.engine.Engine;
import ru.dorofeev.sandbox.quartzworkflow.engine.EngineFactory;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorServiceFactory;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManagerFactory;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStoreFactory;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializationFactory;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepositoryFactory;

public class Factory {

	public static Engine createInMemory() {

		JobRepository jobRepository = JobRepositoryFactory.create();

		ExecutorService executorService = ExecutorServiceFactory.createFixedThreaded(10, 1000);

		QueueStore queueStore = QueueStoreFactory.createInMemoryStore();
		QueueManager queueManager = QueueManagerFactory.create(Factory.class.getName(), queueStore);

		SerializedObjectFactory serializedObjectFactory = SerializationFactory.json();

		return EngineFactory.create(jobRepository, executorService, queueManager, serializedObjectFactory);
	}
}
