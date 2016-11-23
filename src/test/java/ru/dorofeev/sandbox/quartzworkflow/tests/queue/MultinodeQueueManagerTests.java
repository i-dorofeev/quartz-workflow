package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManagerFactory;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueStoreFactory;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.subjects.PublishSubject;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static ru.dorofeev.sandbox.quartzworkflow.JobId.jobId;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.enqueueCmd;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.notifyCompletedCmd;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.JobPoppedEvent;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static rx.schedulers.Schedulers.computation;

public class MultinodeQueueManagerTests {

	private PublishSubject<QueueManager.Cmd> cmdFlow1;
	private PublishSubject<QueueManager.Cmd> cmdFlow2;
	private TestSubscriber<QueueManager.Event> eventSubscriber;
	private TestSubscriber<String> errorSubscriber;
	private QueueStore store;

	@Before
	public void beforeTest() {
		cmdFlow1 = PublishSubject.create();
		cmdFlow2 = PublishSubject.create();
		eventSubscriber = new TestSubscriber<>();
		errorSubscriber = new TestSubscriber<>();

		store = QueueStoreFactory.inMemoryQueueStore();
	}

	@Test
	public void sanityTest() {

		QueueManager queueManager1 = QueueManagerFactory.create("qm1", store);
		Observable<QueueManager.Event> qm1Events = queueManager1.bind(cmdFlow1);
		Observable<String> qm1Errors = queueManager1.getErrors().map(Throwable::getMessage);

		QueueManager queueManager2 = QueueManagerFactory.create("qm2", store);
		Observable<QueueManager.Event> qm2Events = queueManager2.bind(cmdFlow2);
		Observable<String> qm2Errors = queueManager2.getErrors().map(Throwable::getMessage);

		qm1Errors.mergeWith(qm2Errors).subscribe(errorSubscriber);
		qm1Events.mergeWith(qm2Events).subscribe(eventSubscriber);

		qm1Events.observeOn(computation()).subscribe(event -> {
			QueueManager.JobPoppedEvent tpe = (QueueManager.JobPoppedEvent) event;
			cmdFlow2.onNext(notifyCompletedCmd(tpe.getJobId()));
		});

		qm2Events.observeOn(computation()).subscribe(event -> {
			QueueManager.JobPoppedEvent tpe = (QueueManager.JobPoppedEvent) event;
			cmdFlow1.onNext(notifyCompletedCmd(tpe.getJobId()));
		});

		IntStream.range(0, 10)
			.mapToObj(i -> enqueueCmd(EXCLUSIVE, jobId("job" + i)))
			.forEach(cmd -> cmdFlow1.onNext(cmd));

		List<QueueManager.Event> expectedEvents = IntStream.range(0, 10)
			.mapToObj(i -> JobPoppedEvent(jobId("job" + i)))
			.collect(toList());

		eventSubscriber.awaitValueCount(10, 1, SECONDS);
		eventSubscriber.assertReceivedOnNext(expectedEvents);
		errorSubscriber.assertNoValues();
	}
}
