package ru.dorofeev.sandbox.quartzworkflow;

import ru.dorofeev.sandbox.quartzworkflow.engine.Engine;
import ru.dorofeev.sandbox.quartzworkflow.engine.EngineFactory;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepositoryFactory;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManagerFactory;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStore;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import ru.dorofeev.sandbox.quartzworkflow.utils.Clock;
import ru.dorofeev.sandbox.quartzworkflow.utils.SystemClock;
import rx.functions.Func1;

public class Factory {

	public static Engine spawn(SerializedObjectFactory serializedObjectFactory, Func1<SerializedObjectFactory, JobStore> jobStoreFactory, QueueStore queueStore, ExecutorService executorService) {
		Clock clock = new SystemClock();
		JobStore jobStore = jobStoreFactory.call(serializedObjectFactory);
		JobRepository jobRepository = JobRepositoryFactory.create(jobStore, clock);
		QueueManager queueManager = QueueManagerFactory.create(Factory.class.getName(), queueStore);
		return EngineFactory.create(jobRepository, executorService, queueManager);
	}
}
