package ru.dorofeev.sandbox.quartzworkflow.utils;

import java.util.Date;

import static java.lang.System.currentTimeMillis;

public class SystemClock implements Clock {

	@Override
	public Date currentTime() {
		return new Date(currentTimeMillis());
	}
}
