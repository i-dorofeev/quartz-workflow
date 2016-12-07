package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;
import ru.dorofeev.sandbox.quartzworkflow.NodeSpecification;

import java.util.Optional;

public interface QueueStore {

	QueueItem insertQueueItem(JobId jobId, String queueName, QueueingOptions.ExecutionType executionType, NodeSpecification nodeSpecification) throws QueueStoreException;
	Optional<JobId> popNextPendingQueueItem(String queueName, NodeId nodeId);
	Optional<String> releaseQueueItem(JobId jobId);
}
