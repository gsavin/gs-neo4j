package org.graphstream.stream.db;

public class DatabaseConnectionException extends Exception {
	private static final long serialVersionUID = -8612784493661690409L;

	public DatabaseConnectionException(Throwable cause) {
		super(cause);
	}

	public DatabaseConnectionException(String message, Object... args) {
		super(String.format(message, args));
	}
	
	public DatabaseConnectionException() {
		
	}
}
