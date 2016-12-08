package ru.dorofeev.sandbox.quartzworkflow.engine;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

class LocalJobExecutionContext {

	private final Engine.LocalJob localJob;
	private final CompletableFuture<Void> future = new CompletableFuture<>();

	LocalJobExecutionContext(Engine.LocalJob localJob) {
		this.localJob = localJob;
	}

	List<Event> invoke() {
		try {
			List<Event> result = localJob.invoke();
			future.complete(null);
			return result;
		} catch (Exception e) {
			future.completeExceptionally(e);
			throw e;
		}
	}

	public Future<Void> getFuture() {
		return future;
	}
}
