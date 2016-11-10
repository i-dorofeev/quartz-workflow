package ru.dorofeev.sandbox.quartzworkflow;

import java.util.Optional;

public interface QueueStore {

	void insertQueueItem(TaskId taskId, String queueName, QueueingOption.ExecutionType executionType) throws QueueStoreException;
	Optional<TaskId> getNextPendingQueueItem(String queueName);
	Optional<String> removeQueueItem(TaskId taskId);
}
