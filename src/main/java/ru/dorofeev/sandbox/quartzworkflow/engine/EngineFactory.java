package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import ru.dorofeev.sandbox.quartzworkflow.taskrepo.TaskRepository;

public class EngineFactory {

	public static Engine create(TaskRepository taskRepository, ExecutorService executorService, QueueManager queueManager, SerializedObjectFactory serializedObjectFactory) {
		return new EngineImpl(taskRepository, executorService, queueManager, serializedObjectFactory);
	}
}
