package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.JobDataMap;
import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import ru.dorofeev.sandbox.quartzworkflow.execution.Executable;
import ru.dorofeev.sandbox.quartzworkflow.taskrepo.Task;
import ru.dorofeev.sandbox.quartzworkflow.utils.JsonUtils;

import java.util.Optional;

class ExecuteEventHandlerJob implements Executable {

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

	private final EngineImpl engine;

	ExecuteEventHandlerJob(EngineImpl engine) {
		this.engine = engine;
	}

	@Override
	public void execute(JobDataMap args) throws Throwable {
		String eventHandlerUri = args.get(PARAM_EVENT_HANDLER_URI);
		String eventClassName = args.get(PARAM_EVENT_CLASS);
		String eventJson = args.get(PARAM_EVENT_JSON_DATA);
		String taskId = args.get(Task.TASK_ID);

		Event event = JsonUtils.toObject(eventClassName, eventJson);

		Optional<EventHandler> handler = engine.findHandlerByUri(eventHandlerUri);
		handler
			.orElseThrow(() -> new EngineException("No handler found for uri " + eventHandlerUri))
			.handleEvent(event)
			.forEach(e -> engine.submitEvent(new TaskId(taskId), e));
	}
}
