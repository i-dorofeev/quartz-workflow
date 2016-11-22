package ru.dorofeev.sandbox.quartzworkflow.utils;

import java.util.Random;
import java.util.UUID;

public class UUIDGenerator {

	private final Random random = new Random();

	public String newUuid() {
		return new UUID(random.nextLong(), random.nextLong()).toString();
	}
}
