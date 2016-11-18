package ru.dorofeev.sandbox.quartzworkflow.serialization;

public class SerializationFactory {

	public static SerializedObjectFactory jsonSerialization() {
		return new JsonSerializedObjectFactory();
	}
}
