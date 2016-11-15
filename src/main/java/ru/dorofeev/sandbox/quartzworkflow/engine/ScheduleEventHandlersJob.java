package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.JobDataMap;
import ru.dorofeev.sandbox.quartzworkflow.TaskId;
import ru.dorofeev.sandbox.quartzworkflow.execution.Executable;
import ru.dorofeev.sandbox.quartzworkflow.taskrepo.Task;
import ru.dorofeev.sandbox.quartzworkflow.utils.JsonUtils;

import java.util.Set;

class ScheduleEventHandlersJob implements Executable {

	private static final String PARAM_EVENT_CLASS = "eventClass";
	private static final String PARAM_EVENT_JSON_DATA = "eventJsonData";

	static JobDataMap params(Event event) {
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(ScheduleEventHandlersJob.PARAM_EVENT_CLASS, event.getClass().getName());
		jobDataMap.put(ScheduleEventHandlersJob.PARAM_EVENT_JSON_DATA, JsonUtils.toJson(event));
		return jobDataMap;
	}

	private final EngineImpl engine;

	ScheduleEventHandlersJob(EngineImpl engine) {
		this.engine = engine;
	}

	@Override
	public void execute(JobDataMap args) throws ClassNotFoundException {
		String eventClassName = args.get(PARAM_EVENT_CLASS);
		String eventJson = args.get(PARAM_EVENT_JSON_DATA);
		String taskId = args.get(Task.TASK_ID);

		Event event = JsonUtils.toObject(eventClassName, eventJson);

		Set<String> handlers = engine.findHandlers(event.getClass());

		handlers.forEach(eh -> engine.submitHandler(new TaskId(taskId), event, eh));
	}
}
