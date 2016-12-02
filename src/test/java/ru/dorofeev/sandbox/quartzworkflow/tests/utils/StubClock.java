package ru.dorofeev.sandbox.quartzworkflow.tests.utils;

import ru.dorofeev.sandbox.quartzworkflow.utils.Clock;

import java.util.Date;

public class StubClock implements Clock {

	private Date time = new Date();

	public void setTime(Date time) {
		this.time = time;
	}

	public Date getTime() {
		return time;
	}

	@Override
	public Date currentTime() {
		return time;
	}
}
