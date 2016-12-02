package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;

import java.util.Optional;

public interface QueueStore {

	QueueItem insertQueueItem(JobId jobId, String queueName, QueueingOptions.ExecutionType executionType, NodeId nodeId) throws QueueStoreException;
	Optional<JobId> popNextPendingQueueItem(String queueName, NodeId nodeId);
	Optional<String> releaseQueueItem(JobId jobId);
}
