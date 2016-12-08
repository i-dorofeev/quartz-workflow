package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.execution.Executable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;

import java.util.Optional;

public class ExecuteLocalJobJob implements Executable {

	private final EngineImpl engine;

	ExecuteLocalJobJob(EngineImpl engine) {
		this.engine = engine;
	}

	@Override
	public void execute(JobId jobId, SerializedObject serializedArgs) throws Throwable {
		Args args = Args.deserializeFrom(serializedArgs);

		Optional<LocalJobExecutionContext> localJob = engine.getLocalJob(args.localJobId);

		localJob.orElseThrow(() -> new EngineException("Local job [id=" + args.localJobId + "] not found."))
			.invoke()
			.forEach(e -> engine.submitEvent(jobId, e));
	}

	static class Args implements Serializable {

		private final String localJobId;

		Args(String localJobId) {
			this.localJobId = localJobId;
		}

		@Override
		public void serializeTo(SerializedObject serializedObject) {
			serializedObject.addString("localJobId", localJobId);
		}

		static Args deserializeFrom(SerializedObject serializedObject) {
			return new Args(
				serializedObject.getString("localJobId")
			);
		}
	}
}
