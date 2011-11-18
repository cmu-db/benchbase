package com.oltpbenchmark.benchmarks.tpcc;

public final class UserAbortException extends Exception {
	private static final long serialVersionUID = -2275111122558728591L;

	public UserAbortException(String message) {
		super(message);
	}
}