package ru.dorofeev.sandbox.quartzworkflow.execution;

public class ExecutorServiceFactory {

	public static ExecutorService fixedThreadedExecutorService(int nThreads, long idleInterval) {
		return new FixedThreadedExecutorService(nThreads, idleInterval);
	}
}
