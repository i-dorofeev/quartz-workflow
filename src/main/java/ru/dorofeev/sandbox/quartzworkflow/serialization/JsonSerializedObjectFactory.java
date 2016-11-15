package ru.dorofeev.sandbox.quartzworkflow.serialization;

public class JsonSerializedObjectFactory implements SerializedObjectFactory {

	@Override
	public SerializedObject spawn() {
		return new JsonSerializedObject();
	}
}
