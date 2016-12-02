package ru.dorofeev.sandbox.quartzworkflow.execution;

import ru.dorofeev.sandbox.quartzworkflow.utils.Clock;
import ru.dorofeev.sandbox.quartzworkflow.utils.StopwatchFactory;

public class ExecutorServiceFactory {

	public static ExecutorService fixedThreadedExecutorService(int nThreads, long idleInterval, StopwatchFactory stopwatchFactory, Clock clock) {
		return new FixedThreadedExecutorService(nThreads, idleInterval, stopwatchFactory, clock);
	}
}
