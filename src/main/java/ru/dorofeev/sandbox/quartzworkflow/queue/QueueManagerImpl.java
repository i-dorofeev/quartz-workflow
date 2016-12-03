package ru.dorofeev.sandbox.quartzworkflow.queue;

import org.springframework.util.Assert;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;
import ru.dorofeev.sandbox.quartzworkflow.utils.ErrorObservable;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.Optional;

import static ru.dorofeev.sandbox.quartzworkflow.NodeId.ANY_NODE;
import static ru.dorofeev.sandbox.quartzworkflow.utils.Contracts.shouldNotBe;
import static ru.dorofeev.sandbox.quartzworkflow.utils.Contracts.shouldNotBeNull;

class QueueManagerImpl implements QueueManager {

	private final PublishSubject<Event> events = PublishSubject.create();
	private final ErrorObservable errors = new ErrorObservable();

	private final QueueStore queueStore;
	private final NodeId nodeId;

	private boolean suspended;

	QueueManagerImpl(QueueStore queueStore, NodeId nodeId) {
		shouldNotBeNull(queueStore, "queueStore should be specified");
		shouldNotBeNull(nodeId, "nodeId should be specified");
		shouldNotBe(nodeId.equals(ANY_NODE), "nodeId shouldn't be ANY_NODE");

		this.queueStore = queueStore;
		this.nodeId = nodeId;
	}

	@Override
	public Observable<Throwable> getErrors() {
		return errors.asObservable();
	}

	@Override
	public rx.Observable<Event> bind(rx.Observable<Cmd> input) {

		Assert.notNull(input, "input should be specified");

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
		Optional<String> queueName = queueStore.releaseQueueItem(cmd.getJobId());
		queueName.ifPresent(this::tryPushNext);
	}

	private void enqueue(EnqueueCmd cmd) {
		try {
			queueStore.insertQueueItem(cmd.getJobId(), cmd.getQueueName(), cmd.getExecutionType(), cmd.getNodeId());

			if (cmd.getNodeId().equals(this.nodeId) || cmd.getNodeId().equals(ANY_NODE))
				tryPushNext(cmd.getQueueName());
		} catch (QueueStoreException e) {
			errors.asObserver().onNext(new QueueManagerException(e.getMessage(), e));
		}
	}

	private void tryPushNext(String queueName) {
		if (suspended)
			return;

		Optional<JobId> nextOpt = queueStore.popNextPendingQueueItem(queueName, nodeId);
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
			"nodeId=" + nodeId +
			'}';
	}
}
