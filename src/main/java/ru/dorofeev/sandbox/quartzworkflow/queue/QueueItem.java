package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.utils.entrypoint.API;

@API
public interface QueueItem {

	@API
	JobId getJobId();

	@API
	Long getOrdinal();

	@API
	String getQueueName();

	@API
	QueueingOptions.ExecutionType getExecutionType();

	@API
	QueueItemStatus getStatus();
}
