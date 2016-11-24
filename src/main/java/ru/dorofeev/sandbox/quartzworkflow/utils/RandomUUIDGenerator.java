package ru.dorofeev.sandbox.quartzworkflow.utils;

import java.util.Random;
import java.util.UUID;

public class RandomUUIDGenerator implements UUIDGenerator {

	private final Random random = new Random();

	@Override
	public String newUuid() {
		return new UUID(random.nextLong(), random.nextLong()).toString();
	}
}
