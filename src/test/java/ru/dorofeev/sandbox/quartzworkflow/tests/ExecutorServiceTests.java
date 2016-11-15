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
import static ru.dorofeev.sandbox.quartzworkflow.JobId.taskId;
import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.scheduleTaskCmd;
import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.taskFailedEvent;
import static ru.dorofeev.sandbox.quartzworkflow.execution.ExecutorService.taskSuccessfullyCompletedEvent;

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

		cmdFlow.onNext(scheduleTaskCmd(taskId("task0"), null, testExecutable));

		eventTestSubscriber.awaitValueCount(1, 500, MILLISECONDS);
		eventTestSubscriber.assertValuesAndClear(taskSuccessfullyCompletedEvent(taskId("task0")));
		testExecutable.assertInvoked();
	}

	@Test
	public void reportingErrorTest() {

		RuntimeException exception = new RuntimeException("Error!");
		TestExecutable testRunnable = new TestExecutable().throwsException(exception);

		cmdFlow.onNext(scheduleTaskCmd(taskId("task0"), null, testRunnable));

		eventTestSubscriber.awaitValueCount(1, 500, MILLISECONDS);
		eventTestSubscriber.assertValuesAndClear(taskFailedEvent(taskId("task0"), exception));
		testRunnable.assertInvoked();
	}
}
