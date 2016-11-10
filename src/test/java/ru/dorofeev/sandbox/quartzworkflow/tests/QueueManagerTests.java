package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.ObservableHolder;
import ru.dorofeev.sandbox.quartzworkflow.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.QueueManager.Cmd;
import ru.dorofeev.sandbox.quartzworkflow.QueueManager.Event;
import rx.observers.TestSubscriber;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static ru.dorofeev.sandbox.quartzworkflow.QueueManager.enqueueCmd;
import static ru.dorofeev.sandbox.quartzworkflow.QueueManager.taskPoppedEvent;
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

		testSubscriber.assertReceivedOnNext(singletonList(taskPoppedEvent(taskId("task"))));
	}

	@Test
	public void simpleParallelQueueingTest() {

		cmdFlow.onNext(enqueueCmd(PARALLEL, taskId("task1")));
		cmdFlow.onNext(enqueueCmd(PARALLEL, taskId("task2")));

		testSubscriber.assertReceivedOnNext(asList(taskPoppedEvent(taskId("task1")), taskPoppedEvent(taskId("task2"))));
	}




}
