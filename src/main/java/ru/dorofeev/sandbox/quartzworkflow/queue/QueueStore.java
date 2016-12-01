package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.JobId;

import java.util.Optional;

public interface QueueStore {

	QueueItem insertQueueItem(JobId jobId, String queueName, QueueingOptions.ExecutionType executionType) throws QueueStoreException;
	Optional<JobId> popNextPendingQueueItem(String queueName);
	Optional<String> releaseQueueItem(JobId jobId);
}
