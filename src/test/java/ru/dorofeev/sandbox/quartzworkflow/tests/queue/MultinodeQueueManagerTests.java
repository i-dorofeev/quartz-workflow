package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManagerFactory;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.HSqlServices;
import rx.Observable;
import rx.Scheduler;
import rx.observers.TestSubscriber;
import rx.subjects.PublishSubject;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static ru.dorofeev.sandbox.quartzworkflow.JobId.jobId;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.*;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static rx.schedulers.Schedulers.from;

public class MultinodeQueueManagerTests {

	private static final int EVENT_COUNT = 100;

	private PublishSubject<QueueManager.Cmd> qm1Cmds;
	private PublishSubject<QueueManager.Cmd> qm2Cmds;
	private TestSubscriber<QueueManager.Event> eventSubscriber;
	private TestSubscriber<String> errorSubscriber;
	private HSqlServices hSqlServices;

	@Before
	public void beforeTest() {
		qm1Cmds = PublishSubject.create();
		qm2Cmds = PublishSubject.create();
		eventSubscriber = new TestSubscriber<>();
		errorSubscriber = new TestSubscriber<>();

		hSqlServices = new HSqlServices();
	}

	@After
	public void afterTest() {
		hSqlServices.shutdown();
	}

	@Test
	public void sanityTest() {

		// configure
		QueueManager queueManager1 = QueueManagerFactory.create(new NodeId("qm1"), hSqlServices.queueStore());
		Observable<QueueManager.Event> qm1Events = queueManager1.bind(qm1Cmds);
		Observable<String> qm1Errors = queueManager1.getErrors().map(Throwable::getMessage);

		QueueManager queueManager2 = QueueManagerFactory.create(new NodeId("qm2"), hSqlServices.queueStore());
		Observable<QueueManager.Event> qm2Events = queueManager2.bind(qm2Cmds);
		Observable<String> qm2Errors = queueManager2.getErrors().map(Throwable::getMessage);

		qm1Errors.mergeWith(qm2Errors).subscribe(errorSubscriber);
		qm1Events.mergeWith(qm2Events).subscribe(eventSubscriber);

		// observe on a seperate thread in order to avoid StackOverflowException
		Scheduler singleThread = from(Executors.newFixedThreadPool(1));

		qm1Events.observeOn(singleThread).subscribe(event -> {
			QueueManager.JobPoppedEvent tpe = (QueueManager.JobPoppedEvent) event;
			qm2Cmds.onNext(notifyCompletedCmd(tpe.getJobId()));
		});

		qm2Events.observeOn(singleThread).subscribe(event -> {
			QueueManager.JobPoppedEvent tpe = (QueueManager.JobPoppedEvent) event;
			qm1Cmds.onNext(notifyCompletedCmd(tpe.getJobId()));
		});

		// when
		queueManager1.suspend();
		queueManager2.suspend();

		IntStream.range(0, EVENT_COUNT)
			.mapToObj(i -> enqueueCmd(EXCLUSIVE, jobId("job" + i)))
			.forEach(cmd -> qm1Cmds.onNext(cmd));

		queueManager1.resume();
		queueManager2.resume();
		qm1Cmds.onNext(giveMeMoreCmd());

		eventSubscriber.awaitValueCount(EVENT_COUNT, 7, SECONDS);

		// then
		List<QueueManager.Event> expectedEvents = IntStream.range(0, EVENT_COUNT)
			.mapToObj(i -> jobPoppedEvent(jobId("job" + i)))
			.collect(toList());

		eventSubscriber.assertReceivedOnNext(expectedEvents);
		errorSubscriber.assertNoValues();
	}
}
