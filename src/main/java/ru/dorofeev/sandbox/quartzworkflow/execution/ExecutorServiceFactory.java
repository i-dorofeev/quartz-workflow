package ru.dorofeev.sandbox.quartzworkflow.execution;

import ru.dorofeev.sandbox.quartzworkflow.NodeId;
import ru.dorofeev.sandbox.quartzworkflow.utils.Clock;
import ru.dorofeev.sandbox.quartzworkflow.utils.StopwatchFactory;

@FunctionalInterface
public interface ExecutorServiceFactory {

	ExecutorService spawn(NodeId nodeId);

	static ExecutorServiceFactory fixedThreadedExecutorService(int nThreads, long idleInterval, StopwatchFactory stopwatchFactory, Clock clock) {
		return nodeId -> new FixedThreadedExecutorService(nodeId, nThreads, idleInterval, stopwatchFactory, clock);
	}
}
