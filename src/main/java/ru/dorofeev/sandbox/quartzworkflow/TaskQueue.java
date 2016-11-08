package ru.dorofeev.sandbox.quartzworkflow;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.PARALLEL;

class TaskQueue {

	private static class QueueItem {

		final TaskId taskId;
		final QueueingOption.ExecutionType executionType;

		private QueueItem(TaskId taskId, QueueingOption.ExecutionType executionType) {
			this.taskId = taskId;
			this.executionType = executionType;
		}
	}

	private final ObservableHolder<TaskId> observableHolder = new ObservableHolder<>();
	private final Object sync = new Object();

	// persistence
	private final Queue<QueueItem> queue = new ConcurrentLinkedQueue<>();
	private final List<TaskId> runningParallel = new CopyOnWriteArrayList<>();
	private final List<TaskId> runningExclusive = new CopyOnWriteArrayList<>();


	void enqueue(TaskId taskId, QueueingOption.ExecutionType executionType) {
		synchronized (sync) {
			queue.offer(new QueueItem(taskId, executionType));
			tryPushNext();
		}
	}

	void complete(TaskId taskId) {
		synchronized (sync) {
			runningParallel.remove(taskId);
			runningExclusive.remove(taskId);
			tryPushNext();
		}
	}

	private void tryPushNext() {
		QueueItem nextItem = queue.peek();
		if (nextItem == null)
			return;

		if (nextItem.executionType == PARALLEL && runningExclusive.isEmpty()) {
			runningParallel.add(nextItem.taskId);
			observableHolder.onNext(nextItem.taskId);
			queue.poll();
			tryPushNext();
		} else if (nextItem.executionType == EXCLUSIVE && runningParallel.isEmpty() && runningExclusive.isEmpty()) {
			runningExclusive.add(nextItem.taskId);
			observableHolder.onNext(nextItem.taskId);
			queue.poll();
		}
	}

	rx.Observable<TaskId> queue() {
		return observableHolder.getObservable();
	}
}
