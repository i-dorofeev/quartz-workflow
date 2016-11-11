package ru.dorofeev.sandbox.quartzworkflow.tests.utils;

import org.junit.Assert;
import ru.dorofeev.sandbox.quartzworkflow.Executable;

public class TestExecutable implements Executable {

	private boolean invoked;
	private Throwable exception;

	public void assertInvoked() {
		Assert.assertTrue("runnable wasn't invoked", invoked);
	}

	public TestExecutable throwsException(Throwable e) {
		this.exception = e;
		return this;
	}

	@Override
	public void execute() throws Throwable {
		invoked = true;
		if (exception != null)
			throw exception;
	}
}
