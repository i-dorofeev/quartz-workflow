package ru.dorofeev.sandbox.quartzworkflow.tests.queue;

import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import rx.observers.TestSubscriber;
import rx.subjects.PublishSubject;

import static ru.dorofeev.sandbox.quartzworkflow.JobId.jobId;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.*;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManagerFactory.create;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueStoreFactory.inMemoryQueueStore;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType.PARALLEL;

public class QueueManagerTests {

	private PublishSubject<QueueManager.Cmd> cmdFlow;
	private TestSubscriber<QueueManager.Event> eventSubscriber;
	private TestSubscriber<String> errorSubscriber;

	@Before
	public void beforeTest() {
		cmdFlow = PublishSubject.create();
		eventSubscriber = new TestSubscriber<>();
		errorSubscriber = new TestSubscriber<>();

		QueueManager queueManager = create("QueueManagerTests", inMemoryQueueStore());
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
