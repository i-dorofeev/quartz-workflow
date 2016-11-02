package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Optional;

class ExecuteEventHandlerJob implements Job {

	private static final String PARAM_EVENT_HANDLER_URI = "eventHandlerUri";
	private static final String PARAM_EVENT_CLASS = "eventClass";
	private static final String PARAM_EVENT_JSON_DATA = "eventJsonData";

	static JobDataMap params(Event event, String eventHandlerUri) {
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(ExecuteEventHandlerJob.PARAM_EVENT_CLASS, event.getClass().getName());
		jobDataMap.put(ExecuteEventHandlerJob.PARAM_EVENT_JSON_DATA, JsonUtils.toJson(event));
		jobDataMap.put(ExecuteEventHandlerJob.PARAM_EVENT_HANDLER_URI, eventHandlerUri);
		return jobDataMap;
	}

	private final Engine engine;

	ExecuteEventHandlerJob(Engine engine) {
		this.engine = engine;
	}

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		try {
			String eventHandlerUri = (String) context.getMergedJobDataMap().get(PARAM_EVENT_HANDLER_URI);
			String eventClassName = (String) context.getMergedJobDataMap().get(PARAM_EVENT_CLASS);
			String eventJson = (String) context.getMergedJobDataMap().get(PARAM_EVENT_JSON_DATA);
			String processDataId = (String)context.getMergedJobDataMap().get(ProcessData.PROCESS_DATA_ID);

			Event event = JsonUtils.toObject(eventClassName, eventJson);

			Optional<EventHandler> handler = engine.findHandlerByUri(eventHandlerUri);
			handler
				.orElseThrow(() -> new JobExecutionException("No handler found for uri " + eventHandlerUri))
				.handleEvent(event)
				.forEach(e -> engine.submitEvent(GlobalId.fromString(processDataId), e));
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}
}
