package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.utils.RandomUUIDGenerator;
import ru.dorofeev.sandbox.quartzworkflow.utils.UUIDGenerator;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Optional.ofNullable;

class LocalJobStore {

	private final Map<String, LocalJobExecutionContext> store = new ConcurrentHashMap<>();
	private final UUIDGenerator uuidGenerator = new RandomUUIDGenerator();

	String addJob(LocalJobExecutionContext localJob) {

		LocalJobExecutionContext oldCtx;
		String localJobId;
		do {
			localJobId = uuidGenerator.newUuid();
			oldCtx = store.putIfAbsent(localJobId, localJob);
		} while (oldCtx != null);

		return localJobId;
	}

	Optional<LocalJobExecutionContext> getLocalJobExecutionContext(String id) {
		return ofNullable(store.get(id));
	}
}
