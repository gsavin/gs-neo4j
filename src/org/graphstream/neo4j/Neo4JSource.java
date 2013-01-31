package org.graphstream.neo4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.AdjacencyListGraph;
import org.graphstream.stream.SourceBase;
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

public class Neo4JSource extends SourceBase implements FileSource,
		TransactionEventHandler<Object> {
	GraphDatabaseFactory factory;
	GraphDatabaseService graphDb;

	public Neo4JSource() {
		factory = new GraphDatabaseFactory();
	}

	protected void openDB(String path) {
		if (graphDb != null)
			throw new RuntimeException("A graph database is already opened");

		graphDb = factory.newEmbeddedDatabase(path);
		graphDb.registerTransactionEventHandler(null);
	}

	protected void closeDB() {
		if (graphDb == null)
			throw new RuntimeException("No graph database is opened");

		graphDb.shutdown();
	}

	protected void flushDB() {
		if (graphDb == null)
			return;

		Transaction t = graphDb.beginTx();

		try {
			GlobalGraphOperations op = GlobalGraphOperations.at(graphDb);
			
			for (Node n : op.getAllNodes()) {
				sendNodeAdded(sourceId, Long.toString(n.getId()));
			}
			
			for (Relationship e : op.getAllRelationships()) {
				Node src = e.getStartNode();
				Node trg = e.getEndNode();

				sendEdgeAdded(sourceId, Long.toString(e.getId()),
						Long.toString(src.getId()), Long.toString(trg.getId()),
						false);
			}

			t.success();
		} finally {
			t.finish();
		}
	}

	public void readAll(String fileName) throws IOException {
		openDB(fileName);
		flushDB();
		closeDB();
	}

	public void readAll(URL url) throws IOException {
		throw new UnsupportedOperationException();
	}

	public void readAll(InputStream stream) throws IOException {
		throw new UnsupportedOperationException();
	}

	public void readAll(Reader reader) throws IOException {
		throw new UnsupportedOperationException();
	}

	public void begin(String fileName) throws IOException {
		openDB(fileName);
	}

	public void begin(URL url) throws IOException {
		throw new UnsupportedOperationException();
	}

	public void begin(InputStream stream) throws IOException {
		throw new UnsupportedOperationException();
	}

	public void begin(Reader reader) throws IOException {
		throw new UnsupportedOperationException();
	}

	public boolean nextEvents() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean nextStep() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	public void end() throws IOException {
		closeDB();
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
	
	public static void main(String ... args) throws Exception {
		Neo4JSource src = new Neo4JSource();
		Graph g = new AdjacencyListGraph("g");
		src.addSink(g);
		
		g.display(true);
		
		src.readAll(args[0]);
	}
}
