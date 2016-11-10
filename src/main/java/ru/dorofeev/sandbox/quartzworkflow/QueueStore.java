package ru.dorofeev.sandbox.quartzworkflow;

import java.util.Optional;

public interface QueueStore {

	void insertQueueItem(TaskId taskId, String queueName, QueueingOption.ExecutionType executionType);
	Optional<TaskId> getNextPendingQueueItem(String queueName);
	void removeQueueItem(TaskId taskId);
}
