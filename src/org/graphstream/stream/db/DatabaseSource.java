package org.graphstream.stream.db;

import org.graphstream.stream.Source;

public interface DatabaseSource extends Source {
	/**
	 * Connect to a graph database.
	 * 
	 * @param dbPath
	 *            path to the database
	 * @throws DatabaseConnectionException
	 *             thrown if an error occurs while the source tries to connect
	 */
	void connect(String dbPath) throws DatabaseConnectionException;

	/**
	 * Disconnect a connected database source.
	 */
	void disconnect() throws DatabaseConnectionException;
}
