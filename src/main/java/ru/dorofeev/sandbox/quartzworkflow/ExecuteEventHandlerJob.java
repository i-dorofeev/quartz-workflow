package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Optional;

class ExecuteEventHandlerJob implements Job {

	private static final String PARAM_EVENT_HANDLER_URI = "eventHandlerUri";
	private static final String PARAM_EVENT = "event";

	static JobDataMap params(Event event, String eventHandlerUri) {
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(ExecuteEventHandlerJob.PARAM_EVENT, event);
		jobDataMap.put(ExecuteEventHandlerJob.PARAM_EVENT_HANDLER_URI, eventHandlerUri);
		return jobDataMap;
	}

	private final Engine engine;

	ExecuteEventHandlerJob(Engine engine) {
		this.engine = engine;
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		String eventHandlerUri = (String) context.getMergedJobDataMap().get(PARAM_EVENT_HANDLER_URI);
		Event event = (Event) context.getMergedJobDataMap().get(PARAM_EVENT);

		Optional<EventHandler> handler = engine.findHandlerByUri(eventHandlerUri);
		handler
			.orElseThrow(() -> new JobExecutionException("No handler found for uri " + eventHandlerUri))
			.handleEvent(engine, event);
	}
}
