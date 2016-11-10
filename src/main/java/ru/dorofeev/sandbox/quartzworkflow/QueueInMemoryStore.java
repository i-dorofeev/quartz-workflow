package ru.dorofeev.sandbox.quartzworkflow;

import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.PARALLEL;

public class QueueInMemoryStore implements QueueStore {

	private static class QueueItem {

		final long ordinal;
		final TaskId taskId;
		final QueueingOption.ExecutionType executionType;
		final String queueName;

		QueueItemStatus status;

		QueueItem(long ordinal, TaskId taskId, String queueName, QueueingOption.ExecutionType executionType) {
			this.ordinal = ordinal;
			this.taskId = taskId;
			this.queueName = queueName;
			this.executionType = executionType;
			this.status = QueueItemStatus.PENDING;
		}
	}

	private enum QueueItemStatus { PENDING, POPPED }

	private final Object sync = new Object();
	private final SortedSet<QueueItem> queue = new TreeSet<>((o1, o2) -> Long.compare(o2.ordinal, o1.ordinal));

	private long ordinalSeq = 0;

	@Override
	public void insertQueueItem(TaskId taskId, String queueName, QueueingOption.ExecutionType executionType) {
		synchronized (sync) {
			queue.add(new QueueItem(ordinalSeq++, taskId, queueName, executionType));
		}
	}

	private boolean anyExclusivePopped(String queueName) {
		return queue.stream()
			.filter(qi -> qi.status == QueueItemStatus.POPPED && qi.executionType == EXCLUSIVE && qi.queueName.equals(queueName))
			.count() != 0;
	}

	private boolean anyPopped(String queueName) {
		return queue.stream()
			.filter(qi -> qi.status == QueueItemStatus.POPPED && qi.queueName.equals(queueName))
			.count() != 0;
	}

	private Optional<QueueItem> getNextPending(String queueName) {
		return queue.stream().filter(qi -> qi.queueName.equals(queueName) && qi.status == QueueItemStatus.PENDING).findFirst();
	}

	@Override
	public Optional<TaskId> getNextPendingQueueItem(String queueName) {
		synchronized (sync) {
			Optional<QueueItem> nextItemOpt = getNextPending(queueName);

			return nextItemOpt.flatMap(nextItem -> {
				if (nextItem.executionType == PARALLEL && !anyExclusivePopped(queueName)) {
					nextItem.status = QueueItemStatus.POPPED;
					return of(nextItem.taskId);

				} else if (nextItem.executionType == EXCLUSIVE && !anyPopped(queueName)) {
					nextItem.status = QueueItemStatus.POPPED;
					return of(nextItem.taskId);

				} else {
					return empty();
				}
			});
		}
	}

	@Override
	public void removeQueueItem(TaskId taskId) {
		synchronized (sync) {
			queue.removeIf(queueItem -> queueItem.taskId.equals(taskId));
		}
	}
}
