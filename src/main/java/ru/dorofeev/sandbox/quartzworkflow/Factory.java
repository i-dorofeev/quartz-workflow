package ru.dorofeev.sandbox.quartzworkflow;

import ru.dorofeev.sandbox.quartzworkflow.engine.Engine;
import ru.dorofeev.sandbox.quartzworkflow.engine.EngineFactory;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorServiceFactory;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManagerFactory;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStoreFactory;
import ru.dorofeev.sandbox.quartzworkflow.taskrepo.TaskRepository;
import ru.dorofeev.sandbox.quartzworkflow.taskrepo.TaskRepositoryFactory;

public class Factory {

	public static Engine createInMemory() {

		TaskRepository taskRepository = TaskRepositoryFactory.create();

		ExecutorService executorService = ExecutorServiceFactory.createFixedThreaded(10, 1000);

		QueueStore queueStore = QueueStoreFactory.createInMemoryStore();
		QueueManager queueManager = QueueManagerFactory.create(Factory.class.getName(), queueStore);

		return EngineFactory.create(taskRepository, executorService, queueManager);
	}
}
