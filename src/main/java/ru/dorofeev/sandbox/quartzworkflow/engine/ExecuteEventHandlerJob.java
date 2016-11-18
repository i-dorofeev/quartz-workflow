package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.execution.Executable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;

import java.util.Optional;

class ExecuteEventHandlerJob implements Executable {

	private final EngineImpl engine;

	ExecuteEventHandlerJob(EngineImpl engine) {
		this.engine = engine;
	}

	@Override
	public void execute(JobId jobId, SerializedObject serializedArgs) throws Throwable {

		Args args = Args.deserializeFrom(serializedArgs);

		Optional<EventHandler> handler = engine.findHandlerByUri(args.eventHandlerUri);
		handler
			.orElseThrow(() -> new EngineException("No handler found for uri " + args.eventHandlerUri))
			.handleEvent(args.event)
			.forEach(e -> engine.submitEvent(jobId, e));
	}

	static class Args implements Serializable {

		private final String eventHandlerUri;
		private final Event event;

		Args(String eventHandlerUri, Event event) {
			this.eventHandlerUri = eventHandlerUri;
			this.event = event;
		}

		@Override
		public void serializeTo(SerializedObject serializedObject) {
			serializedObject.addString("eventHandlerUri", eventHandlerUri);
			serializedObject.addUntypedObject("event", event);
		}

		static Args deserializeFrom(SerializedObject serializedObject) {
			return new Args(
				serializedObject.getString("eventHandlerUri"),
				serializedObject.getUntypedObject("event", Event.class));
		}
	}

}
