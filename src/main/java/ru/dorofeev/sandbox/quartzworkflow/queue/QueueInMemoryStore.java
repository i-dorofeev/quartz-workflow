package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.TaskId;

import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;

import static java.util.Optional.empty;
import static java.util.Optional.of;

class QueueInMemoryStore implements QueueStore {

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
	private final SortedSet<QueueItem> queue = new TreeSet<>((o1, o2) -> Long.compare(o1.ordinal, o2.ordinal));

	private long ordinalSeq = 0;

	@Override
	public void insertQueueItem(TaskId taskId, String queueName, QueueingOption.ExecutionType executionType) throws QueueStoreException {
		synchronized (sync) {
			if (queue.stream().filter(qi -> qi.taskId.equals(taskId)).count() > 0)
				throw new QueueStoreException(taskId + " is already enqueued");

			queue.add(new QueueItem(ordinalSeq++, taskId, queueName, executionType));
		}
	}

	private boolean anyExclusivePopped(String queueName) {
		return queue.stream()
			.filter(qi -> qi.status == QueueItemStatus.POPPED && qi.executionType == QueueingOption.ExecutionType.EXCLUSIVE && qi.queueName.equals(queueName))
			.count() != 0;
	}

	private boolean anyPopped(String queueName) {
		return queue.stream()
			.filter(qi -> qi.status == QueueItemStatus.POPPED && qi.queueName.equals(queueName))
			.count() != 0;
	}

	private Optional<QueueItem> getNextPending(Predicate<String> queueNamePredicate) {
		return queue.stream().filter(qi -> queueNamePredicate.test(qi.queueName) && qi.status == QueueItemStatus.PENDING).findFirst();
	}

	@Override
	public Optional<TaskId> getNextPendingQueueItem(String queueName) {
		synchronized (sync) {
			Optional<QueueItem> nextItemOpt = getNextPending(queueName != null ? qn -> qn.equals(queueName) : qn -> true);

			return nextItemOpt.flatMap(nextItem -> {
				if (nextItem.executionType == QueueingOption.ExecutionType.PARALLEL && !anyExclusivePopped(queueName)) {
					nextItem.status = QueueItemStatus.POPPED;
					return of(nextItem.taskId);

				} else if (nextItem.executionType == QueueingOption.ExecutionType.EXCLUSIVE && !anyPopped(queueName)) {
					nextItem.status = QueueItemStatus.POPPED;
					return of(nextItem.taskId);

				} else {
					return empty();
				}
			});
		}
	}

	@Override
	public Optional<String> removeQueueItem(TaskId taskId) {
		synchronized (sync) {
			Optional<QueueItem> queueItem = queue.stream().filter(qi -> qi.taskId.equals(taskId)).findFirst();
			if (queueItem.isPresent()) {
				queue.remove(queueItem.get());
				return of(queueItem.get().queueName);
			} else {
				return empty();
			}
		}
	}
}
