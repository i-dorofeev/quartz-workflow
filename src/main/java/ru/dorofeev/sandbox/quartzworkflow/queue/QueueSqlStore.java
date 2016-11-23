package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.JobId;

import java.util.Optional;

class QueueSqlStore implements QueueStore {

	QueueSqlStore(String dataSourceUrl) {
	}

	@Override
	public void insertQueueItem(JobId jobId, String queueName, QueueingOptions.ExecutionType executionType) throws QueueStoreException {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Optional<JobId> getNextPendingQueueItem(String queueName) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Optional<String> removeQueueItem(JobId jobId) {
		throw new UnsupportedOperationException("Not implemented yet");
	}
}
