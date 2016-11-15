package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.TaskId;

import java.util.Optional;

public interface QueueStore {

	void insertQueueItem(TaskId taskId, String queueName, QueueingOption.ExecutionType executionType) throws QueueStoreException;
	Optional<TaskId> getNextPendingQueueItem(String queueName);
	Optional<String> removeQueueItem(TaskId taskId);
}
