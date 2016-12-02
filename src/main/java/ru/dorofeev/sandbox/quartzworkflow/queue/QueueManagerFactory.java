package ru.dorofeev.sandbox.quartzworkflow.queue;

import ru.dorofeev.sandbox.quartzworkflow.NodeId;

public class QueueManagerFactory {

	public static QueueManager create(NodeId nodeId, QueueStore store) {
		return new QueueManagerImpl(store, nodeId);
	}
}
