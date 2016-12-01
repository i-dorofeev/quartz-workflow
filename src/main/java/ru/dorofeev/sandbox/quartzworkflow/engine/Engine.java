package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.*;
import ru.dorofeev.sandbox.quartzworkflow.jobs.Job;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepository;

public interface Engine {

	JobKey SCHEDULE_EVENT_HANDLERS_JOB = new JobKey("SCHEDULE_EVENT_HANDLERS_JOB");
	JobKey EXECUTE_EVENT_HANDLER_JOB = new JobKey("EXECUTE_EVENT_HANDLER_JOB");

	rx.Observable<Throwable> errors();

	JobRepository getJobRepository();

	void start();

	void shutdown();

	Job submitEvent(Event event);

	void retryExecution(JobId jobId);

	void registerEventHandlerInstance(String handlerUri, EventHandler eventHandler);

	void registerEventHandler(Class<? extends Event> eventType, String handlerUri);

	void registerEventHandler(Class<? extends Event> cmdEventType, EventHandler cmdHandler, String handlerUri);
}
