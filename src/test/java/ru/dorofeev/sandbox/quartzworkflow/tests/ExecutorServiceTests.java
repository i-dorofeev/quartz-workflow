package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.Assert;
import org.junit.Test;
import ru.dorofeev.sandbox.quartzworkflow.ExecutorService;
import ru.dorofeev.sandbox.quartzworkflow.ExecutorService.Cmd;
import ru.dorofeev.sandbox.quartzworkflow.ObservableHolder;
import rx.observers.TestSubscriber;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static ru.dorofeev.sandbox.quartzworkflow.ExecutorService.scheduleTaskCmd;
import static ru.dorofeev.sandbox.quartzworkflow.ExecutorService.taskSuccessfullyCompletedEvent;
import static ru.dorofeev.sandbox.quartzworkflow.TaskId.taskId;

public class ExecutorServiceTests {

	@Test
	public void sanityTest() {

		ExecutorService executorService = new ExecutorService(5);

		ObservableHolder<Cmd> cmdFlow = new ObservableHolder<>();
		TestSubscriber<ExecutorService.Event> eventTestSubscriber = new TestSubscriber<>();

		executorService.bind(cmdFlow.getObservable()).subscribe(eventTestSubscriber);

		TestRunnable testRunnable = new TestRunnable();
		cmdFlow.onNext(scheduleTaskCmd(taskId("task0"), testRunnable));

		eventTestSubscriber.awaitValueCount(1, 500, MILLISECONDS);
		eventTestSubscriber.assertValuesAndClear(taskSuccessfullyCompletedEvent(taskId("task0")));
		testRunnable.assertInvoked();
	}

	private static class TestRunnable implements Runnable {

		private boolean invoked;

		@Override
		public void run() {
			invoked = true;
		}

		public void assertInvoked() {
			Assert.assertTrue("runnable wasn't invoked", invoked);
		}
	}
}
