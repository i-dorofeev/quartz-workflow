package ru.dorofeev.sandbox.quartzworkflow;

import java.util.Optional;

class TaskQueue {

	private final String queueName;

	private final ObservableHolder<TaskId> observableHolder = new ObservableHolder<>();
	private final QueueStore queueStore;

	TaskQueue(QueueStore queueStore, String queueName) {
		this.queueStore = queueStore;
		this.queueName = queueName;
	}

	void enqueue(TaskId taskId, QueueingOption.ExecutionType executionType) throws QueueStoreException {
		queueStore.insertQueueItem(taskId, queueName, executionType);
		tryPushNext();
	}

	void complete(TaskId taskId) {
		queueStore.removeQueueItem(taskId);
		tryPushNext();
	}

	void tryPushNext() {
		Optional<TaskId> nextOpt = queueStore.getNextPendingQueueItem(queueName);
		nextOpt.ifPresent(observableHolder::onNext);
	}

	rx.Observable<TaskId> queue() {
		return observableHolder.getObservable();
	}
}
