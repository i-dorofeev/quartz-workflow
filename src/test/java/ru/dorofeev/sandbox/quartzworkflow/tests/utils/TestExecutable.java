package ru.dorofeev.sandbox.quartzworkflow.tests.utils;

import org.junit.Assert;
import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import ru.dorofeev.sandbox.quartzworkflow.execution.Executable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TestExecutable implements Executable {

	private final AtomicInteger invocationCount = new AtomicInteger(0);
	private final Random random = new Random();
	private Throwable exception;
	private int duration;
	private int accuracy;

	public void assertInvoked() {
		Assert.assertNotEquals("executable wasn't invoked", 0, invocationCount.intValue());
	}

	public TestExecutable throwsException(Throwable e) {
		this.exception = e;
		return this;
	}

	public TestExecutable withExecutionDuration(int durationMs, int accuracyMs) {
		if (durationMs <= 0 || accuracyMs <= 0 || durationMs <= accuracyMs)
			throw new IllegalArgumentException("Wrong duration (" + durationMs + " ms) and accuracy (" + accuracyMs + " ms)");

		this.duration = durationMs;
		this.accuracy = accuracyMs;
		return this;
	}

	@Override
	public void execute(TaskId taskId, SerializedObject serializedArgs) throws Throwable {
		invocationCount.incrementAndGet();

		if (duration > 0)
			Thread.sleep(duration - accuracy + random.nextInt(accuracy * 2));

		if (exception != null)
			throw exception;
	}
}
