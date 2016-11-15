package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import ru.dorofeev.sandbox.quartzworkflow.utils.ErrorObservable;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.Optional;

class QueueManagerImpl implements QueueManager {

	private final PublishSubject<Event> events = PublishSubject.create();
	private final ErrorObservable errors = new ErrorObservable();

	private final String name;
	private final QueueStore queueStore;

	QueueManagerImpl(String name, QueueStore queueStore) {
		this.name = name;
		this.queueStore = queueStore;
	}

	@Override
	public Observable<Throwable> getErrors() {
		return errors.asObservable();
	}

	@Override
	public rx.Observable<Event> bind(rx.Observable<Cmd> input) {

		input.ofType(EnqueueCmd.class)
			.subscribe(this::enqueue);

		input.ofType(NotifyCompletedCmd.class)
			.subscribe(this::notifyCompleted);

		input.ofType(GiveMeMoreCmd.class)
			.subscribe(cmd -> requestNewTasks());

		return events;
	}

	private void requestNewTasks() {
		tryPushNext(null);
	}

	private void notifyCompleted(NotifyCompletedCmd cmd) {
		Optional<String> queueName = queueStore.removeQueueItem(cmd.getTaskId());
		queueName.ifPresent(this::tryPushNext);
	}

	private void enqueue(EnqueueCmd cmd) {
		try {
			queueStore.insertQueueItem(cmd.getTaskId(), cmd.getQueueName(), cmd.getExecutionType());
			tryPushNext(cmd.getQueueName());
		} catch (QueueStoreException e) {
			errors.asObserver().onNext(new QueueManagerException(e.getMessage(), e));
		}
	}

	private void tryPushNext(String queueName) {
		Optional<TaskId> nextOpt = queueStore.getNextPendingQueueItem(queueName);
		nextOpt
			.map(TaskPoppedEvent::new)
			.ifPresent(tpe -> {
				events.onNext(tpe);
				tryPushNext(queueName);
			});
	}

	@Override
	public String toString() {
		return "QueueManagerImpl{" +
			"name='" + name + '\'' +
			'}';
	}
}
