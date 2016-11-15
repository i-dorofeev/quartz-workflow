package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.JobId;

import java.util.Optional;

public interface QueueStore {

	void insertQueueItem(JobId jobId, String queueName, QueueingOption.ExecutionType executionType) throws QueueStoreException;
	Optional<JobId> getNextPendingQueueItem(String queueName);
	Optional<String> removeQueueItem(JobId jobId);
}
