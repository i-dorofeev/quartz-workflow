package ru.dorofeev.sandbox.quartzworkflow.serialization;

public interface SerializedObjectFactory {

	SerializedObject spawn();
	SerializedObject spawn(String src);
}
