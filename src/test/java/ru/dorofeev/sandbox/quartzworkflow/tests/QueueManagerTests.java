package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.ObservableHolder;
import ru.dorofeev.sandbox.quartzworkflow.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.QueueManager.*;
import rx.observers.TestSubscriber;

import static ru.dorofeev.sandbox.quartzworkflow.QueueManager.*;
import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.PARALLEL;
import static ru.dorofeev.sandbox.quartzworkflow.TaskId.taskId;

public class QueueManagerTests {

	private ObservableHolder<Cmd> cmdFlow;
	private TestSubscriber<Event> testSubscriber;

	@Before
	public void beforeTest() {
		cmdFlow = new ObservableHolder<>();
		testSubscriber = new TestSubscriber<>();

		QueueManager queueManager = new QueueManager();
		queueManager.bindEvents(cmdFlow.getObservable()).subscribe(testSubscriber);
	}

	@Test
	public void sanityTest() {

		cmdFlow.onNext(enqueueCmd(taskId("task")));

		testSubscriber.assertValuesAndClear(taskPoppedEvent(taskId("task")));
	}

	@Test
	public void simpleParallelQueueingTest() {

		cmdFlow.onNext(enqueueCmd(PARALLEL, taskId("task1")));
		cmdFlow.onNext(enqueueCmd(PARALLEL, taskId("task2")));

		testSubscriber.assertValuesAndClear(taskPoppedEvent(taskId("task1")), taskPoppedEvent(taskId("task2")));
	}

	@Test
	public void simpleSequentialQueueingTest() {

		cmdFlow.onNext(enqueueCmd(EXCLUSIVE, taskId("task1")));
		cmdFlow.onNext(enqueueCmd(EXCLUSIVE, taskId("task2")));

		testSubscriber.assertValuesAndClear(taskPoppedEvent(taskId("task1")));

		cmdFlow.onNext(notifyCompletedCmd(QueueManager.DEFAULT_QUEUE_NAME, taskId("task1")));

		testSubscriber.assertValuesAndClear(taskPoppedEvent(taskId("task2")));
	}


}
