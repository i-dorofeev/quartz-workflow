package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import ru.dorofeev.sandbox.quartzworkflow.engine.EngineException;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.Optional;

class QueueManagerImpl implements QueueManager {



	private final PublishSubject<Event> outputHolder = PublishSubject.create();
	private final PublishSubject<Exception> errorOutputHolder = PublishSubject.create();

	private final String name;
	private final QueueStore queueStore;

	QueueManagerImpl(String name, QueueStore queueStore) {
		this.name = name;
		this.queueStore = queueStore;
	}

	@Override
	public Observable<Exception> errors() {
		return errorOutputHolder;
	}

	@Override
	public rx.Observable<Event> bind(rx.Observable<Cmd> input) {
		input.subscribe(cmd -> {

			if (cmd instanceof EnqueueCmd)
				enqueue((EnqueueCmd) cmd);

			else if (cmd instanceof NotifyCompletedCmd)
				notifyCompleted((NotifyCompletedCmd) cmd);

			else if (cmd instanceof GiveMeMoreCmd)
				requestNewTasks();

			else
				errorOutputHolder.onNext(new EngineException("Unrecognized cmd " + cmd));
		});

		return outputHolder;
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
			errorOutputHolder.onNext(new EngineException(e.getMessage(), e));
		}
	}

	private void tryPushNext(String queueName) {
		Optional<TaskId> nextOpt = queueStore.getNextPendingQueueItem(queueName);
		nextOpt
			.map(TaskPoppedEvent::new)
			.ifPresent(tpe -> {
				outputHolder.onNext(tpe);
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
