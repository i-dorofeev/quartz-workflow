package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.utils.ErrorObservable;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.Optional;

class QueueManagerImpl implements QueueManager {

	private final PublishSubject<Event> events = PublishSubject.create();
	private final ErrorObservable errors = new ErrorObservable();

	private final String name;
	private final QueueStore queueStore;

	private boolean suspended;

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
			.subscribe(cmd -> giveMeMoreCmd());

		return events;
	}

	private void giveMeMoreCmd() {
		tryPushNext(null);
	}

	private void notifyCompleted(NotifyCompletedCmd cmd) {
		Optional<String> queueName = queueStore.removeQueueItem(cmd.getJobId());
		queueName.ifPresent(this::tryPushNext);
	}

	private void enqueue(EnqueueCmd cmd) {
		try {
			queueStore.insertQueueItem(cmd.getJobId(), cmd.getQueueName(), cmd.getExecutionType());
			tryPushNext(cmd.getQueueName());
		} catch (QueueStoreException e) {
			errors.asObserver().onNext(new QueueManagerException(e.getMessage(), e));
		}
	}

	private void tryPushNext(String queueName) {
		if (suspended)
			return;

		Optional<JobId> nextOpt = queueStore.popNextPendingQueueItem(queueName);
		nextOpt
			.map(JobPoppedEvent::new)
			.ifPresent(tpe -> {
				events.onNext(tpe);
				tryPushNext(queueName);
			});
	}

	@Override
	public void suspend() {
		this.suspended = true;
	}

	@Override
	public void resume() {
		this.suspended = false;
	}

	@Override
	public String toString() {
		return "QueueManagerImpl{" +
			"name='" + name + '\'' +
			'}';
	}
}
