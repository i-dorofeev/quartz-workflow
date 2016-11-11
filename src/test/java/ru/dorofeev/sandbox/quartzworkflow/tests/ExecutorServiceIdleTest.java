package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.ExecutorService.Cmd;
import ru.dorofeev.sandbox.quartzworkflow.ExecutorService.Event;
import ru.dorofeev.sandbox.quartzworkflow.ExecutorService.IdleEvent;
import ru.dorofeev.sandbox.quartzworkflow.ExecutorService.TaskCompletedEvent;
import ru.dorofeev.sandbox.quartzworkflow.ObservableHolder;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.TestExecutable;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.TestStorage;
import rx.Observable;
import rx.observers.TestSubscriber;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static ru.dorofeev.sandbox.quartzworkflow.ExecutorService.scheduleTaskCmd;
import static ru.dorofeev.sandbox.quartzworkflow.TaskId.taskId;
import static rx.Observable.range;
import static rx.schedulers.Schedulers.io;

public class ExecutorServiceIdleTest {

	private ObservableHolder<Cmd> cmdFlow;
	private TestSubscriber<Event> eventTestSubscriber;
	private TestExecutable testExecutable;
	private TestStorage<Event> eventStorage;

	@Before
	public void beforeTest() {
		cmdFlow = new ObservableHolder<>();
		eventTestSubscriber = new TestSubscriber<>();

		testExecutable = new TestExecutable().withExecutionDuration(5, 3);
		eventStorage = new TestStorage<>();
	}

	@Test
	public void sanityTest() throws Exception {

		ExecutorService executorService = new ExecutorService(10, 10);

		Observable<Event> executorEvents = executorService.bind(cmdFlow.getObservable())
			.subscribeOn(io());

		executorEvents.subscribe(eventStorage::add);

		Observable<IdleEvent> idleEvents = executorEvents.ofType(IdleEvent.class);
		Observable<TaskCompletedEvent> taskCompletedEvents = executorEvents.ofType(TaskCompletedEvent.class);

		// idle
		Thread.sleep(100);

		// emitting scheduleTaskCmd in response to the first 50 idle events
		idleEvents
			.flatMap(idleEvent -> range(0, idleEvent.getFreeThreadsCount()))
			.map(i -> scheduleTaskCmd(taskId("task"), testExecutable))
			.take(50)
			.subscribe(cmdFlow.nextObserver());

		taskCompletedEvents
			.subscribe(eventTestSubscriber);

		eventTestSubscriber.awaitValueCount(50, 1000, MILLISECONDS);

		// idle
		Thread.sleep(100);

		eventStorage.assertNextItemsOfTypeMin(IdleEvent.class, 10);
		eventStorage.assertNextItemsOfTypeExact(TaskCompletedEvent.class, 50);
		eventStorage.assertNextItemsOfTypeMin(IdleEvent.class, 10);
	}
}
