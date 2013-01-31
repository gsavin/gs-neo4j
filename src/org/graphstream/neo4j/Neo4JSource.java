package org.graphstream.neo4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.AdjacencyListGraph;
import org.graphstream.stream.SourceBase;
import org.graphstream.stream.db.DatabaseConnectionException;
import org.graphstream.stream.db.DatabaseSource;
import org.graphstream.stream.file.FileSource;
import org.graphstream.util.VerboseSink;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class Neo4JSource extends SourceBase implements DatabaseSource,
		TransactionEventHandler<Object> {
	GraphDatabaseFactory factory;
	GraphDatabaseService graphDb;

	public Neo4JSource() {
		factory = new GraphDatabaseFactory();
		Runtime.getRuntime().addShutdownHook(new CloseDbOnExit());
	}

	/**
	 * Read all the database content and produce events that describe the
	 * current graph state.
	 */
	protected void flushDB() {
		if (graphDb == null)
			return;

		Transaction t = graphDb.beginTx();

		try {
			GlobalGraphOperations op = GlobalGraphOperations.at(graphDb);

			for (Node n : op.getAllNodes()) {
				String nodeId = Long.toString(n.getId());
				sendNodeAdded(sourceId, nodeId);

				for (String key : n.getPropertyKeys())
					sendNodeAttributeAdded(sourceId, nodeId, key,
							n.getProperty(key));
			}

			for (Relationship e : op.getAllRelationships()) {
				String edgeId = Long.toString(e.getId());
				Node src = e.getStartNode();
				Node trg = e.getEndNode();

				sendEdgeAdded(sourceId, edgeId, Long.toString(src.getId()),
						Long.toString(trg.getId()), false);

				for (String key : e.getPropertyKeys())
					sendEdgeAttributeAdded(sourceId, edgeId, key,
							e.getProperty(key));
			}

			t.success();
		} catch (Throwable e) {
			t.failure();
		} finally {
			t.finish();
		}
	}

	public void connect(String dbPath) throws DatabaseConnectionException {
		if (graphDb != null)
			throw new DatabaseConnectionException(
					"A graph database is already opened");

		graphDb = factory.newEmbeddedDatabase(dbPath);
		graphDb.registerTransactionEventHandler(this);

		flushDB();
	}

	public void disconnect() throws DatabaseConnectionException {
		if (graphDb == null)
			throw new DatabaseConnectionException("No graph database is opened");

		graphDb.shutdown();
		graphDb = null;
	}

	public void afterCommit(TransactionData arg0, Object arg1) {
		// TODO Auto-generated method stub

	}

	public void afterRollback(TransactionData arg0, Object arg1) {
		// TODO Auto-generated method stub

	}

	public Object beforeCommit(TransactionData arg0) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	private class CloseDbOnExit extends Thread {
		public void run() {
			if (graphDb != null)
				try {
					disconnect();
				} catch (DatabaseConnectionException e) {

				}
		}
	}

	public static void main(String... args) throws Exception {
		Neo4JSource src = new Neo4JSource();
		Graph g = new AdjacencyListGraph("g");

		g.addAttribute("ui.quality");
		g.addAttribute("ui.antilias");

		src.addSink(g);

		g.display(true);

		//src.addSink(new VerboseSink());

		if (args != null && args.length > 0)
			src.connect(args[0]);
		else
			src.connect("dataset/twitter");
		
		src.disconnect();
	}
}
