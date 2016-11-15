package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.Cmd;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.Event;
import ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorServiceFactory;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.TestExecutable;
import rx.observers.TestSubscriber;
import rx.subjects.PublishSubject;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static ru.dorofeev.sandbox.quartzworkflow.JobId.jobId;
import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.scheduleJobCmd;
import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.jobFailedEvent;
import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.jobSuccessfullyCompletedEvent;

public class ExecutorServiceTests {

	private PublishSubject<Cmd> cmdFlow;
	private TestSubscriber<Event> eventTestSubscriber;

	@Before
	public void beforeTest() {
		ExecutorService executorService = ExecutorServiceFactory.createFixedThreaded(5, 50);

		cmdFlow = PublishSubject.create();
		eventTestSubscriber = new TestSubscriber<>();

		executorService.bind(cmdFlow)
			.filter(e -> !(e instanceof ExecutorService.IdleEvent))
			.subscribe(eventTestSubscriber);
	}

	@Test
	public void sanityTest() {

		TestExecutable testExecutable = new TestExecutable();

		cmdFlow.onNext(scheduleJobCmd(jobId("job0"), null, testExecutable));

		eventTestSubscriber.awaitValueCount(1, 500, MILLISECONDS);
		eventTestSubscriber.assertValuesAndClear(jobSuccessfullyCompletedEvent(jobId("job0")));
		testExecutable.assertInvoked();
	}

	@Test
	public void reportingErrorTest() {

		RuntimeException exception = new RuntimeException("Error!");
		TestExecutable testRunnable = new TestExecutable().throwsException(exception);

		cmdFlow.onNext(scheduleJobCmd(jobId("job0"), null, testRunnable));

		eventTestSubscriber.awaitValueCount(1, 500, MILLISECONDS);
		eventTestSubscriber.assertValuesAndClear(jobFailedEvent(jobId("job0"), exception));
		testRunnable.assertInvoked();
	}
}
