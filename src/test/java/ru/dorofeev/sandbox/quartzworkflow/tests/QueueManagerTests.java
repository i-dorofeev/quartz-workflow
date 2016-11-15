package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager;
import rx.observers.TestSubscriber;
import rx.subjects.PublishSubject;

import static ru.dorofeev.sandbox.quartzworkflow.TaskId.taskId;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManager.*;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueManagerFactory.create;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueStoreFactory.createInMemoryStore;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption.ExecutionType.PARALLEL;

public class QueueManagerTests {

	private PublishSubject<QueueManager.Cmd> cmdFlow;
	private TestSubscriber<QueueManager.Event> eventSubscriber;
	private TestSubscriber<String> errorSubscriber;

	@Before
	public void beforeTest() {
		cmdFlow = PublishSubject.create();
		eventSubscriber = new TestSubscriber<>();
		errorSubscriber = new TestSubscriber<>();

		QueueManager queueManager = create("QueueManagerTests", createInMemoryStore());
		queueManager.bind(cmdFlow).subscribe(eventSubscriber);
		queueManager.errors().map(Throwable::getMessage).subscribe(errorSubscriber);
	}

	@Test
	public void sanityTest() {

		cmdFlow.onNext(enqueueCmd(taskId("task")));
		eventSubscriber.assertValuesAndClear(taskPoppedEvent(taskId("task")));
		errorSubscriber.assertNoValues();

		cmdFlow.onNext(giveMeMoreCmd());
		eventSubscriber.assertNoValues();
		errorSubscriber.assertNoValues();
	}

	@Test
	public void simpleParallelQueueingTest() {

		cmdFlow.onNext(enqueueCmd(PARALLEL, taskId("task1")));
		cmdFlow.onNext(enqueueCmd(PARALLEL, taskId("task2")));

		eventSubscriber.assertValuesAndClear(taskPoppedEvent(taskId("task1")), taskPoppedEvent(taskId("task2")));
		errorSubscriber.assertNoValues();
	}

	@Test
	public void simpleSequentialQueueingTest() {

		cmdFlow.onNext(enqueueCmd(EXCLUSIVE, taskId("task1")));
		cmdFlow.onNext(enqueueCmd(EXCLUSIVE, taskId("task2")));

		eventSubscriber.assertValuesAndClear(taskPoppedEvent(taskId("task1")));

		cmdFlow.onNext(notifyCompletedCmd(taskId("task1")));

		eventSubscriber.assertValuesAndClear(taskPoppedEvent(taskId("task2")));

		errorSubscriber.assertNoValues();
	}

	@Test
	public void cannotEnqueueSameTaskTwice() {

		cmdFlow.onNext(enqueueCmd(PARALLEL, taskId("task1")));
		cmdFlow.onNext(enqueueCmd(PARALLEL, taskId("task1")));

		eventSubscriber.assertValuesAndClear(taskPoppedEvent(taskId("task1")));
		errorSubscriber.assertValuesAndClear("task1 is already enqueued");
	}
}
