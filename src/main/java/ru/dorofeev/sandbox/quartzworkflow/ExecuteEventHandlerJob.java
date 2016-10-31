package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

class ExecuteEventHandlerJob implements Job {

	static final String PARAM_EVENT_HANDLER_CLASS = "eventHandlerClass";
	static final String PARAM_EVENT = "event";

	private final Engine engine;

	ExecuteEventHandlerJob(Engine engine) {
		this.engine = engine;
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		//noinspection unchecked
		Class<? extends EventHandler> eventHandlerClass = (Class<? extends EventHandler>) context.getMergedJobDataMap().get(PARAM_EVENT_HANDLER_CLASS);

		Event event = (Event) context.getMergedJobDataMap().get(PARAM_EVENT);

		try {
			EventHandler eventHandler = eventHandlerClass.newInstance();
			eventHandler.handleEvent(engine, event);
		} catch (InstantiationException | IllegalAccessException e) {
			throw new JobExecutionException(e);
		}
	}
}
