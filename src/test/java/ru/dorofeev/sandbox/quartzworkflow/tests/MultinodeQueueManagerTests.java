package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.QueueInMemoryStore;
import ru.dorofeev.sandbox.quartzworkflow.QueueManager;
import ru.dorofeev.sandbox.quartzworkflow.QueueManager.*;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.subjects.PublishSubject;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static ru.dorofeev.sandbox.quartzworkflow.QueueManager.*;
import static ru.dorofeev.sandbox.quartzworkflow.QueueingOption.ExecutionType.EXCLUSIVE;
import static ru.dorofeev.sandbox.quartzworkflow.TaskId.taskId;
import static rx.schedulers.Schedulers.computation;

public class MultinodeQueueManagerTests {

	private PublishSubject<Cmd> cmdFlow1;
	private PublishSubject<Cmd> cmdFlow2;
	private TestSubscriber<QueueManager.Event> eventSubscriber;
	private TestSubscriber<String> errorSubscriber;
	private QueueInMemoryStore store;

	@Before
	public void beforeTest() {
		cmdFlow1 = PublishSubject.create();
		cmdFlow2 = PublishSubject.create();
		eventSubscriber = new TestSubscriber<>();
		errorSubscriber = new TestSubscriber<>();

		store = new QueueInMemoryStore();
	}

	@Test
	public void sanityTest() {

		QueueManager queueManager1 = new QueueManager("qm1", store);
		Observable<QueueManager.Event> qm1Events = queueManager1.bindEvents(cmdFlow1);
		Observable<String> qm1Errors = queueManager1.errors().map(Throwable::getMessage);

		QueueManager queueManager2 = new QueueManager("qm2", store);
		Observable<QueueManager.Event> qm2Events = queueManager2.bindEvents(cmdFlow2);
		Observable<String> qm2Errors = queueManager2.errors().map(Throwable::getMessage);

		qm1Errors.mergeWith(qm2Errors).subscribe(errorSubscriber);
		qm1Events.mergeWith(qm2Events).subscribe(eventSubscriber);

		qm1Events.observeOn(computation()).subscribe(event -> {
			TaskPoppedEvent tpe = (TaskPoppedEvent) event;
			cmdFlow2.onNext(notifyCompletedCmd(tpe.getTaskId()));
		});

		qm2Events.observeOn(computation()).subscribe(event -> {
			TaskPoppedEvent tpe = (TaskPoppedEvent) event;
			cmdFlow1.onNext(notifyCompletedCmd(tpe.getTaskId()));
		});

		IntStream.range(0, 10)
			.mapToObj(i -> enqueueCmd(EXCLUSIVE, taskId("task" + i)))
			.forEach(cmd -> cmdFlow1.onNext(cmd));

		List<QueueManager.Event> expectedEvents = IntStream.range(0, 10)
			.mapToObj(i -> taskPoppedEvent(taskId("task" + i)))
			.collect(toList());

		eventSubscriber.awaitValueCount(10, 1, SECONDS);
		eventSubscriber.assertReceivedOnNext(expectedEvents);
		errorSubscriber.assertNoValues();
	}
}
