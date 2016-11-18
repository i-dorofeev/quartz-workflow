package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.execution.Executable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;

import java.util.Set;

class ScheduleEventHandlersJob implements Executable {

	private final EngineImpl engine;

	ScheduleEventHandlersJob(EngineImpl engine) {
		this.engine = engine;
	}

	@Override
	public void execute(JobId jobId, SerializedObject serializedArgs) throws ClassNotFoundException {
		Args args = Args.deserializeFrom(serializedArgs);
		Set<String> handlers = engine.findHandlers(args.event.getClass());

		handlers.forEach(eh -> engine.submitHandler(jobId, args.event, eh));
	}

	static class Args implements Serializable {

		private final Event event;

		Args(Event event) {
			this.event = event;
		}

		@Override
		public void serializeTo(SerializedObject serializedObject) {
			serializedObject.addUntypedObject("event", event);
		}

		static Args deserializeFrom(SerializedObject serializedObject) {
			return new Args(
				serializedObject.getUntypedObject("event", Event.class)
			);
		}
	}
}
