package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.JobId;

import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;

import static java.util.Comparator.comparingLong;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

class InMemoryQueueStore implements QueueStore {

	private static class InMemoryQueueItem implements QueueItem {

		final long ordinal;
		final JobId jobId;
		final QueueingOptions.ExecutionType executionType;
		final String queueName;

		QueueItemStatus status;

		InMemoryQueueItem(long ordinal, JobId jobId, String queueName, QueueingOptions.ExecutionType executionType) {
			this.ordinal = ordinal;
			this.jobId = jobId;
			this.queueName = queueName;
			this.executionType = executionType;
			this.status = QueueItemStatus.PENDING;
		}

		@Override
		public JobId getJobId() {
			return jobId;
		}

		@Override
		public Long getOrdinal() {
			return ordinal;
		}

		@Override
		public String getQueueName() {
			return queueName;
		}

		@Override
		public QueueingOptions.ExecutionType getExecutionType() {
			return executionType;
		}

		@Override
		public QueueItemStatus getStatus() {
			return status;
		}
	}

	private final Object sync = new Object();
	private final SortedSet<InMemoryQueueItem> queue = new TreeSet<>(comparingLong(o -> o.ordinal));

	private long ordinalSeq = 0;

	@Override
	public QueueItem insertQueueItem(JobId jobId, String queueName, QueueingOptions.ExecutionType executionType) throws QueueStoreException {
		synchronized (sync) {
			if (queue.stream().filter(qi -> qi.jobId.equals(jobId)).count() > 0)
				throw new QueueStoreException(jobId + " is already enqueued");

			InMemoryQueueItem inMemoryQueueItem = new InMemoryQueueItem(++ordinalSeq, jobId, queueName, executionType);
			queue.add(inMemoryQueueItem);
			return inMemoryQueueItem;
		}
	}

	private boolean anyExclusivePopped(String queueName) {
		return queue.stream()
			.filter(qi -> qi.queueName.equals(queueName))
			.filter(qi -> qi.status == QueueItemStatus.POPPED && qi.executionType == QueueingOptions.ExecutionType.EXCLUSIVE)
			.count() != 0;
	}

	private boolean anyPopped(String queueName) {
		return queue.stream()
			.filter(qi -> qi.queueName.equals(queueName))
			.filter(qi -> qi.status == QueueItemStatus.POPPED)
			.count() != 0;
	}

	private Optional<InMemoryQueueItem> getNextPending(Predicate<String> queueNamePredicate) {
		return queue.stream()
			.filter(qi -> queueNamePredicate.test(qi.queueName))
			.filter(qi -> qi.status == QueueItemStatus.PENDING)
			.findFirst();
	}

	@Override
	public Optional<JobId> popNextPendingQueueItem(String queueName) {
		synchronized (sync) {
			Optional<InMemoryQueueItem> nextItemOpt = getNextPending(queueName != null ? queueName::equals : qn -> true);

			return nextItemOpt.flatMap(nextItem -> {
				if (nextItem.executionType == QueueingOptions.ExecutionType.PARALLEL && !anyExclusivePopped(nextItem.queueName)) {
					nextItem.status = QueueItemStatus.POPPED;
					return of(nextItem.jobId);

				} else if (nextItem.executionType == QueueingOptions.ExecutionType.EXCLUSIVE && !anyPopped(nextItem.queueName)) {
					nextItem.status = QueueItemStatus.POPPED;
					return of(nextItem.jobId);

				} else {
					return empty();
				}
			});
		}
	}

	@Override
	public Optional<String> releaseQueueItem(JobId jobId) {
		synchronized (sync) {
			Optional<InMemoryQueueItem> queueItem = queue.stream().filter(qi -> qi.jobId.equals(jobId)).findFirst();
			if (queueItem.isPresent()) {
				queue.remove(queueItem.get());
				return ofNullable(queueItem.get().queueName);
			} else {
				return empty();
			}
		}
	}
}
