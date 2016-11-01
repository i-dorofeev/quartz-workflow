package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.*;

import java.util.Set;

class ScheduleEventHandlersJob implements Job {

	private static final String PARAM_EVENT = "event";

	static JobDataMap params(Event event) {
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(ScheduleEventHandlersJob.PARAM_EVENT, event);
		return jobDataMap;
	}

	private final Engine engine;

	ScheduleEventHandlersJob(Engine engine) {
		this.engine = engine;
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		Event event = (Event)context.getMergedJobDataMap().get(PARAM_EVENT);
		Set<String> handlers = engine.findHandlers(event.getClass());

		handlers.forEach(eh -> engine.submitHandler(event, eh));
	}
}
