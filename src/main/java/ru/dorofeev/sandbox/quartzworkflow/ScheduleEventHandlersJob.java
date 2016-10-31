package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

import java.util.Set;

class ScheduleEventHandlersJob implements Job {

	static final String PARAM_EVENT = "event";

	private final Engine engine;

	ScheduleEventHandlersJob(Engine engine) {
		this.engine = engine;
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		Event event = (Event)context.getMergedJobDataMap().get(PARAM_EVENT);
		Set<Class<? extends EventHandler>> handlers = engine.findHandlers(event.getClass());

		handlers.forEach(eh -> {
			try {
				engine.submitHandler(event, eh);
			} catch (SchedulerException e) {
				throw new RuntimeException(e);
			}
		});
	}
}
