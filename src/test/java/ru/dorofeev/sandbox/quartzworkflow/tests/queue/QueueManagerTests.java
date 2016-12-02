package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.queue.sql.SqlQueueStore;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.HSqlServices;
import rx.observers.TestSubscriber;
import rx.subjects.PublishSubject;

import static ru.dorofeev.sandbox.quartzworkflow.JobId.jobId;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.*;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManagerFactory.create;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;

public class QueueManagerTests {

	private static HSqlServices hSqlServices;

	private PublishSubject<QueueManager.Cmd> cmdFlow;
	private TestSubscriber<QueueManager.Event> eventSubscriber;
	private TestSubscriber<String> errorSubscriber;

	@BeforeClass
	public static void beforeClass() {
		hSqlServices = new HSqlServices();
	}

	@AfterClass
	public static void afterClass() {
		hSqlServices.shutdown();
	}

	@Before
	public void beforeTest() {
		cmdFlow = PublishSubject.create();
		eventSubscriber = new TestSubscriber<>();
		errorSubscriber = new TestSubscriber<>();

		SqlQueueStore sqlQueueStore = hSqlServices.queueStore();
		sqlQueueStore.clear();

		QueueManager queueManager = create(new NodeId("QueueManagerTests"), sqlQueueStore);
		queueManager.bind(cmdFlow).subscribe(eventSubscriber);
		queueManager.getErrors().map(Throwable::getMessage).subscribe(errorSubscriber);
	}

	@Test
	public void sanityTest() {

		cmdFlow.onNext(enqueueCmd(jobId("job")));
		eventSubscriber.assertValuesAndClear(jobPoppedEvent(jobId("job")));
		errorSubscriber.assertNoValues();

		cmdFlow.onNext(giveMeMoreCmd());
		eventSubscriber.assertNoValues();
		errorSubscriber.assertNoValues();
	}

	@Test
	public void simpleParallelQueueingTest() {

		cmdFlow.onNext(enqueueCmd(PARALLEL, jobId("job1")));
		cmdFlow.onNext(enqueueCmd(PARALLEL, jobId("job2")));

		eventSubscriber.assertValuesAndClear(jobPoppedEvent(jobId("job1")), jobPoppedEvent(jobId("job2")));
		errorSubscriber.assertNoValues();
	}

	@Test
	public void simpleSequentialQueueingTest() {

		cmdFlow.onNext(enqueueCmd(EXCLUSIVE, jobId("job1")));
		cmdFlow.onNext(enqueueCmd(EXCLUSIVE, jobId("job2")));

		eventSubscriber.assertValuesAndClear(jobPoppedEvent(jobId("job1")));

		cmdFlow.onNext(notifyCompletedCmd(jobId("job1")));

		eventSubscriber.assertValuesAndClear(jobPoppedEvent(jobId("job2")));

		errorSubscriber.assertNoValues();
	}

	@Test
	public void cannotEnqueueSameJobTwice() {

		cmdFlow.onNext(enqueueCmd(PARALLEL, jobId("job1")));
		cmdFlow.onNext(enqueueCmd(PARALLEL, jobId("job1")));

		eventSubscriber.assertValuesAndClear(jobPoppedEvent(jobId("job1")));
		errorSubscriber.assertValuesAndClear("job1 is already enqueued");
	}
}
