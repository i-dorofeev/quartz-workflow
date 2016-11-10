package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.ObservableHolder;
import ru.dorofeev.sandbox.quartzworkflow.QueueInMemoryStore;
import ru.dorofeev.sandbox.quartzworkflow.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.QueueManager.*;
import rx.observers.TestSubscriber;

import static ru.dorofeev.sandbox.quartzworkflow.QueueManager.*;
import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.PARALLEL;
import static ru.dorofeev.sandbox.quartzworkflow.TaskId.taskId;

public class QueueManagerTests {

	private ObservableHolder<Cmd> cmdFlow;
	private TestSubscriber<Event> eventSubscriber;
	private TestSubscriber<String> errorSubscriber;

	@Before
	public void beforeTest() {
		cmdFlow = new ObservableHolder<>();
		eventSubscriber = new TestSubscriber<>();
		errorSubscriber = new TestSubscriber<>();

		QueueManager queueManager = new QueueManager("QueueManagerTests", new QueueInMemoryStore());
		queueManager.bindEvents(cmdFlow.getObservable()).subscribe(eventSubscriber);
		queueManager.errors().map(Throwable::getMessage).subscribe(errorSubscriber);
	}

	@Test
	public void sanityTest() {

		cmdFlow.onNext(enqueueCmd(taskId("task")));
		eventSubscriber.assertValuesAndClear(taskPoppedEvent(taskId("task")));
		errorSubscriber.assertNoValues();

		cmdFlow.onNext(requestNewTasksCmd());
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
