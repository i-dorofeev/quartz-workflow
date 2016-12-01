package ru.dorofeev.sandbox.quartzworkflow.utils;

public class RealtimeStopwatchFactory implements StopwatchFactory {

	@Override
	public Stopwatch newStopwatch() {

		return new Stopwatch() {

			private final long started = System.currentTimeMillis();

			@Override
			public long elapsed() {
				return System.currentTimeMillis() - started;
			}
		};
	}
}
