package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.jobs.Job;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository;

import java.util.List;
import java.util.concurrent.Future;

public interface Engine {

	rx.Observable<Throwable> errors();

	JobRepository getJobRepository();

	void start();

	void shutdown();

	Job submitEvent(Event event);

	Future<Void> submitLocalJob(LocalJob localJob);

	void retryExecution(JobId jobId);

	void registerEventHandlerInstance(String handlerUri, EventHandler eventHandler);

	void registerEventHandler(Class<? extends Event> eventType, String handlerUri);

	void registerEventHandler(Class<? extends Event> cmdEventType, EventHandler cmdHandler, String handlerUri);

	@FunctionalInterface
	interface LocalJob {

		List<Event> invoke();
	}
}
