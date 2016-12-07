package ru.dorofeev.sandbox.quartzworkflow.tests.execution;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.*;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorServiceFactory;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.StubClock;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.TestExecutable;
import ru.dorofeev.sandbox.quartzworkflow.utils.Stopwatch;
import ru.dorofeev.sandbox.quartzworkflow.utils.StopwatchFactory;
import rx.observers.TestSubscriber;
import rx.subjects.PublishSubject;

import java.util.Date;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static ru.dorofeev.sandbox.quartzworkflow.JobId.jobId;
import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.*;

public class ExecutorServiceTests {

	private PublishSubject<Cmd> cmdFlow;
	private TestSubscriber<Event> eventTestSubscriber;

	private ExecutorService executorService;
	private StubStopwatchFactory stopwatchFactory;
	private StubClock clock;

	private static final NodeId node = new NodeId("node");

	@Before
	public void beforeTest() {
		stopwatchFactory = new StubStopwatchFactory();
		clock = new StubClock();
		executorService = ExecutorServiceFactory.fixedThreadedExecutorService(5, 50, stopwatchFactory, clock);

		cmdFlow = PublishSubject.create();
		eventTestSubscriber = new TestSubscriber<>();

		executorService.bind(cmdFlow)
			.filter(e -> !(e instanceof ExecutorService.IdleEvent))
			.subscribe(eventTestSubscriber);

		executorService.start();
	}

	@After
	public void afterTest() {
		executorService.shutdown();
	}

	@Test
	public void sanityTest() {

		TestExecutable testExecutable = new TestExecutable();

		stopwatchFactory.setExpectedElapsed(1000L);
		clock.setTime(new Date());
		cmdFlow.onNext(scheduleJobCmd(jobId("job0"), null, testExecutable));

		eventTestSubscriber.awaitValueCount(1, 500, MILLISECONDS);
		eventTestSubscriber.assertValuesAndClear(jobSuccessfullyCompletedEvent(jobId("job0"), stopwatchFactory.getExpectedElapsed(), clock.getTime(), node));
		testExecutable.assertInvoked();
	}

	@Test
	public void reportingErrorTest() {

		RuntimeException exception = new RuntimeException("Error!");
		TestExecutable testRunnable = new TestExecutable().throwsException(exception);

		stopwatchFactory.setExpectedElapsed(2000L);
		clock.setTime(new Date());
		cmdFlow.onNext(scheduleJobCmd(jobId("job0"), null, testRunnable));

		eventTestSubscriber.awaitValueCount(1, 500, MILLISECONDS);
		eventTestSubscriber.assertValuesAndClear(jobFailedEvent(jobId("job0"), exception, stopwatchFactory.getExpectedElapsed(), clock.getTime(), node));
		testRunnable.assertInvoked();
	}

	private static class StubStopwatchFactory implements StopwatchFactory {

		private long expectedElapsed;

		void setExpectedElapsed(long expectedElapsed) {
			this.expectedElapsed = expectedElapsed;
		}

		long getExpectedElapsed() {
			return expectedElapsed;
		}

		@Override
		public Stopwatch newStopwatch() {
			return () -> expectedElapsed;
		}
	}
}
