package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Set;

class ScheduleEventHandlersJob implements Job {

	private static final String PARAM_EVENT_CLASS = "eventClass";
	private static final String PARAM_EVENT_JSON_DATA = "eventJsonData";

	static JobDataMap params(Event event) {
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(ScheduleEventHandlersJob.PARAM_EVENT_CLASS, event.getClass().getName());
		jobDataMap.put(ScheduleEventHandlersJob.PARAM_EVENT_JSON_DATA, Event.toJson(event));
		return jobDataMap;
	}

	private final Engine engine;

	ScheduleEventHandlersJob(Engine engine) {
		this.engine = engine;
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		try {
			String eventClassName = (String) context.getMergedJobDataMap().get(PARAM_EVENT_CLASS);
			String eventJson = (String) context.getMergedJobDataMap().get(PARAM_EVENT_JSON_DATA);

			Event event = Event.toEvent(eventClassName, eventJson);

			Set<String> handlers = engine.findHandlers(event.getClass());

			handlers.forEach(eh -> engine.submitHandler(event, eh));
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}
}
