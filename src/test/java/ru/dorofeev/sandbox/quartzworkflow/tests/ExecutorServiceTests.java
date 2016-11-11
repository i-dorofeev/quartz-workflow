package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.Before;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.ExecutorService.*;
import ru.dorofeev.sandbox.quartzworkflow.ObservableHolder;
import ru.dorofeev.sandbox.quartzworkflow.tests.utils.TestExecutable;
import rx.observers.TestSubscriber;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static ru.dorofeev.sandbox.quartzworkflow.ExecutorService.*;
import static ru.dorofeev.sandbox.quartzworkflow.TaskId.taskId;

public class ExecutorServiceTests {

	private ObservableHolder<Cmd> cmdFlow;
	private TestSubscriber<Event> eventTestSubscriber;

	@Before
	public void beforeTest() {
		ExecutorService executorService = new ExecutorService(5);

		cmdFlow = new ObservableHolder<>();
		eventTestSubscriber = new TestSubscriber<>();

		executorService.bind(cmdFlow.getObservable())
			.filter(e -> !(e instanceof IdleEvent))
			.subscribe(eventTestSubscriber);
	}

	@Test
	public void sanityTest() {

		TestExecutable testExecutable = new TestExecutable();

		cmdFlow.onNext(scheduleTaskCmd(taskId("task0"), testExecutable));

		eventTestSubscriber.awaitValueCount(1, 500, MILLISECONDS);
		eventTestSubscriber.assertValuesAndClear(taskSuccessfullyCompletedEvent(taskId("task0")));
		testExecutable.assertInvoked();
	}

	@Test
	public void reportingErrorTest() {

		RuntimeException exception = new RuntimeException("Error!");
		TestExecutable testRunnable = new TestExecutable().throwsException(exception);

		cmdFlow.onNext(scheduleTaskCmd(taskId("task0"), testRunnable));

		eventTestSubscriber.awaitValueCount(1, 500, MILLISECONDS);
		eventTestSubscriber.assertValuesAndClear(taskFailedEvent(taskId("task0"), exception));
		testRunnable.assertInvoked();
	}
}