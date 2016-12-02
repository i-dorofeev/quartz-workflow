package ru.dorofeev.sandbox.quartzworkflow.tests.execution;

import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.Cmd;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.Event;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.IdleEvent;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorServiceFactory;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.TestExecutable;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.TestStorage;
import ru.dorofeev.sandbox.quartzworkflow.utils.RealtimeStopwatchFactory;
import ru.dorofeev.sandbox.quartzworkflow.utils.SystemClock;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.subjects.PublishSubject;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static ru.dorofeev.sandbox.quartzworkflow.JobId.jobId;
import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.scheduleJobCmd;
import static rx.Observable.range;
import static rx.schedulers.Schedulers.io;

public class ExecutorServiceIdleTest {

	private PublishSubject<Cmd> cmdFlow;
	private TestSubscriber<Event> eventTestSubscriber;
	private TestExecutable testExecutable;
	private TestStorage<Event> eventStorage;

	@Before
	public void beforeTest() {
		cmdFlow = PublishSubject.create();
		eventTestSubscriber = new TestSubscriber<>();

		testExecutable = new TestExecutable().withExecutionDuration(5, 3);
		eventStorage = new TestStorage<>();
	}

	@Test
	public void sanityTest() throws Exception {

		ExecutorService executorService = ExecutorServiceFactory.fixedThreadedExecutorService(10, 10, new RealtimeStopwatchFactory(), new SystemClock());
		executorService.start();

		Observable<Event> executorEvents = executorService.bind(cmdFlow)
			.subscribeOn(io());

		executorEvents
//			.doOnNext(System.out::println)
			.subscribe(eventStorage::add);

		Observable<IdleEvent> idleEvents = executorEvents.ofType(IdleEvent.class);
		Observable<ExecutorService.JobCompletedEvent> jobCompletedEvents = executorEvents.ofType(ExecutorService.JobCompletedEvent.class);

		// idle
		Thread.sleep(100);

		// emitting scheduleJobCmd in response to the first 50 idle events
		idleEvents
			.flatMap(idleEvent -> range(0, idleEvent.getFreeThreadsCount()))
			.map(i -> scheduleJobCmd(jobId("job"), null, testExecutable))
			.take(50)
			.subscribe(cmdFlow::onNext);	// we shouldn't complete the stream here, so propagating only onNext events

		jobCompletedEvents
			.subscribe(eventTestSubscriber);

		eventTestSubscriber.awaitValueCount(50, 1000, MILLISECONDS);

		// idle
		Thread.sleep(100);

		eventStorage.assertNextItemsOfTypeMin(IdleEvent.class, 10);
		eventStorage.assertNextItemsOfTypeExact(ExecutorService.JobCompletedEvent.class, 50);
		eventStorage.assertNextItemsOfTypeMin(IdleEvent.class, 10);

		executorService.shutdown();
	}
}
