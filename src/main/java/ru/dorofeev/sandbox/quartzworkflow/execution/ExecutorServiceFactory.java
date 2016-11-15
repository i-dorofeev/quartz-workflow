package ru.dorofeev.sandbox.quartzworkflow.execution;

public class ExecutorServiceFactory {

	public static ExecutorService createFixedThreaded(int nThreads, long idleInterval) {
		return new FixedThreadedExecutorService(nThreads, idleInterval);
	}
}
