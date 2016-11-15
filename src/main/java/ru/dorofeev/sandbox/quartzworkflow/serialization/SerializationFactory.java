package ru.dorofeev.sandbox.quartzworkflow.serialization;

public class SerializationFactory {

	public static SerializedObjectFactory json() {
		return new JsonSerializedObjectFactory();
	}
}
