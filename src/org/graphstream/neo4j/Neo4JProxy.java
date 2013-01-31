/*
 * Copyright 2006 - 2013
 *      Stefan Balev       <stefan.balev@graphstream-project.org>
 *      Julien Baudry	<julien.baudry@graphstream-project.org>
 *      Antoine Dutot	<antoine.dutot@graphstream-project.org>
 *      Yoann Pigné	<yoann.pigne@graphstream-project.org>
 *      Guilhelm Savin	<guilhelm.savin@graphstream-project.org>
 *  
 * GraphStream is a library whose purpose is to handle static or dynamic
 * graph, create them from scratch, file or any source and display them.
 * 
 * This program is free software distributed under the terms of two licenses, the
 * CeCILL-C license that fits European law, and the GNU Lesser General Public
 * License. You can  use, modify and/ or redistribute the software under the terms
 * of the CeCILL-C license as circulated by CEA, CNRS and INRIA at the following
 * URL <http://www.cecill.info> or under the terms of the GNU LGPL as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-C and LGPL licenses and that you accept their terms.
 */
package org.graphstream.neo4j;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.graphstream.neo4j.tools.Cache;
import org.graphstream.neo4j.tools.CacheCreationException;
import org.graphstream.stream.SourceBase;
import org.graphstream.stream.db.DatabaseConnectionException;
import org.graphstream.stream.db.DatabaseProxy;
import org.graphstream.stream.sync.SinkTime;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class Neo4JProxy extends SourceBase implements DatabaseProxy,
		TransactionEventHandler<Object> {
	public static final int DEFAULT_NODE_CACHE_SIZE = 1000;
	public static final int DEFAULT_EDGE_CACHE_SIZE = 1000;

	/**
	 * List of opened databases which should be closed if system crash.
	 */
	private static final ConcurrentLinkedQueue<GraphDatabaseService> OPENED_DB = new ConcurrentLinkedQueue<GraphDatabaseService>();

	/**
	 * Register a new opened database.
	 * 
	 * @param db
	 *            the database service
	 */
	protected static void registerDatabase(GraphDatabaseService db) {
		OPENED_DB.add(db);
	}

	/**
	 * Unregister a shutdown database. Use this AFTER calling the
	 * {@link GraphDatabaseService#shutdown()} method of the service.
	 * 
	 * @param db
	 *            the closed database service
	 */
	protected static void unregisterDatabase(GraphDatabaseService db) {
		OPENED_DB.remove(db);
	}

	//
	// Here we register a shutdown hook which is call by the virtual machine
	// even if the application crash. The thread will closed of remaining opened
	// database.
	//
	static {
		Runnable r = new Runnable() {
			public void run() {
				if (OPENED_DB.size() > 0)
					System.err.printf("Closing %d database%s\n",
							OPENED_DB.size(), OPENED_DB.size() > 1 ? "s" : "");

				while (OPENED_DB.size() > 0) {
					GraphDatabaseService db = OPENED_DB.poll();
					db.shutdown();
				}
			}
		};

		Runtime.getRuntime().addShutdownHook(new Thread(r, "close-db"));
	}

	GraphDatabaseFactory factory;
	GraphDatabaseService graphDb;
	Mode mode;
	SinkTime sinkTime;

	NodeCache nodeCache = new NodeCache(DEFAULT_NODE_CACHE_SIZE);
	EdgeCache edgeCache = new EdgeCache(DEFAULT_EDGE_CACHE_SIZE);

	public Neo4JProxy() {
		this.factory = new GraphDatabaseFactory();
	}

	protected String getNodeId(Node dbNode) {
		if (dbNode.hasProperty("id"))
			return dbNode.getProperty("id").toString();

		return Long.toString(dbNode.getId());
	}

	protected String getEdgeId(Relationship dbEdge) {
		if (dbEdge.hasProperty("id"))
			return dbEdge.getProperty("id").toString();

		return Long.toString(dbEdge.getId());
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
				String nodeId = getNodeId(n);
				sendNodeAdded(sourceId, nodeId);

				for (String key : n.getPropertyKeys())
					if (!key.equals("id"))
						sendNodeAttributeAdded(sourceId, nodeId, key,
								n.getProperty(key));
			}

			for (Relationship e : op.getAllRelationships()) {
				String edgeId = getEdgeId(e);
				String src = getNodeId(e.getStartNode());
				String trg = getNodeId(e.getEndNode());

				sendEdgeAdded(sourceId, edgeId, src, trg, false);

				for (String key : e.getPropertyKeys())
					if (!key.equals("id"))
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

	protected Node getDBNode(String nodeId) {
		try {
			return nodeCache.get(nodeId);
		} catch (CacheCreationException e) {
			e.printStackTrace();
			return null;
		}
	}

	protected Relationship getDBEdge(String edgeId) {
		try {
			return edgeCache.get(edgeId);
		} catch (CacheCreationException e) {
			e.printStackTrace();
			return null;
		}
	}

	public ExecutionResult executeCypher(String req) {
		ExecutionEngine engine = new ExecutionEngine(graphDb);
		ExecutionResult result = engine.execute(req);

		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.stream.db.DatabaseProxy#connect(java.lang.String,
	 * org.graphstream.stream.db.DatabaseProxy.Mode)
	 */
	public void connect(String dbPath, Mode mode)
			throws DatabaseConnectionException {
		if (graphDb != null)
			throw new DatabaseConnectionException(
					"A graph database is already opened");

		this.mode = mode;

		graphDb = factory.newEmbeddedDatabase(dbPath);
		graphDb.registerTransactionEventHandler(this);

		registerDatabase(graphDb);

		if (mode != Mode.WRITE_ONLY)
			flushDB();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.stream.db.DatabaseProxy#disconnect()
	 */
	public void disconnect() throws DatabaseConnectionException {
		if (graphDb == null)
			throw new DatabaseConnectionException("No graph database is opened");

		graphDb.shutdown();
		unregisterDatabase(graphDb);
		graphDb = null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.neo4j.graphdb.event.TransactionEventHandler#afterCommit(org.neo4j
	 * .graphdb.event.TransactionData, java.lang.Object)
	 */
	public void afterCommit(TransactionData td, Object arg1) {
		if (mode == Mode.WRITE_ONLY)
			return;

		for (Node n : td.createdNodes())
			sendNodeAdded(sourceId, getNodeId(n));

		for (Relationship r : td.createdRelationships()) {
			String src = getNodeId(r.getStartNode());
			String trg = getNodeId(r.getEndNode());

			sendEdgeAdded(sourceId, getEdgeId(r), src, trg, false);
		}

		for (PropertyEntry<Node> pe : td.assignedNodeProperties()) {
			if (pe.key().equals("id")) {
				idAttributeChanged(pe.entity());
				continue;
			}

			sendNodeAttributeChanged(sourceId, getNodeId(pe.entity()),
					pe.key(), pe.previouslyCommitedValue(), pe.value());
		}

		for (PropertyEntry<Relationship> pe : td
				.assignedRelationshipProperties()) {
			if (pe.key().equals("id")) {
				idAttributeChanged(pe.entity());
				continue;
			}

			sendEdgeAttributeChanged(sourceId, getEdgeId(pe.entity()),
					pe.key(), pe.previouslyCommitedValue(), pe.value());
		}

		for (PropertyEntry<Node> pe : td.removedNodeProperties()) {
			if (pe.key().equals("id")) {
				idAttributeChanged(pe.entity());
				continue;
			}

			sendNodeAttributeRemoved(sourceId, getNodeId(pe.entity()), pe.key());
		}

		for (PropertyEntry<Relationship> pe : td
				.removedRelationshipProperties()) {
			if (pe.key().equals("id")) {
				idAttributeChanged(pe.entity());
				continue;
			}

			sendEdgeAttributeRemoved(sourceId, getEdgeId(pe.entity()), pe.key());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.neo4j.graphdb.event.TransactionEventHandler#afterRollback(org.neo4j
	 * .graphdb.event.TransactionData, java.lang.Object)
	 */
	public void afterRollback(TransactionData arg0, Object arg1) {
		if (mode == Mode.WRITE_ONLY)
			return;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.neo4j.graphdb.event.TransactionEventHandler#beforeCommit(org.neo4j
	 * .graphdb.event.TransactionData)
	 */
	public Object beforeCommit(TransactionData td) throws Exception {
		for (Node n : td.deletedNodes())
			nodeCache.revokeKey(getNodeId(n));

		for (Relationship r : td.deletedRelationships())
			edgeCache.revokeKey(getEdgeId(r));

		if (mode == Mode.WRITE_ONLY)
			return null;

		for (Relationship r : td.deletedRelationships())
			sendEdgeRemoved(sourceId, getEdgeId(r));

		for (Node n : td.deletedNodes())
			sendNodeRemoved(sourceId, getNodeId(n));

		return null;
	}

	protected void idAttributeChanged(Node n) {
		// TODO
		throw new UnsupportedOperationException();
	}

	protected void idAttributeChanged(Relationship r) {
		// TODO
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.graphstream.stream.AttributeSink#graphAttributeAdded(java.lang.String
	 * , long, java.lang.String, java.lang.Object)
	 */
	public void graphAttributeAdded(String sourceId, long timeId,
			String attribute, Object value) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.graphstream.stream.AttributeSink#graphAttributeChanged(java.lang.
	 * String, long, java.lang.String, java.lang.Object, java.lang.Object)
	 */
	public void graphAttributeChanged(String sourceId, long timeId,
			String attribute, Object oldValue, Object newValue) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.graphstream.stream.AttributeSink#graphAttributeRemoved(java.lang.
	 * String, long, java.lang.String)
	 */
	public void graphAttributeRemoved(String sourceId, long timeId,
			String attribute) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.graphstream.stream.AttributeSink#nodeAttributeAdded(java.lang.String,
	 * long, java.lang.String, java.lang.String, java.lang.Object)
	 */
	public void nodeAttributeAdded(String sourceId, long timeId, String nodeId,
			String attribute, Object value) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;

		Node n = getDBNode(nodeId);
		Transaction t = graphDb.beginTx();

		try {
			n.setProperty(attribute, value);
			t.success();
		} catch (Throwable e) {
			t.failure();
		} finally {
			t.finish();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.graphstream.stream.AttributeSink#nodeAttributeChanged(java.lang.String
	 * , long, java.lang.String, java.lang.String, java.lang.Object,
	 * java.lang.Object)
	 */
	public void nodeAttributeChanged(String sourceId, long timeId,
			String nodeId, String attribute, Object oldValue, Object newValue) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;

		Node n = getDBNode(nodeId);
		Transaction t = graphDb.beginTx();

		try {
			n.setProperty(attribute, newValue);
			t.success();
		} catch (Throwable e) {
			t.failure();
		} finally {
			t.finish();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.graphstream.stream.AttributeSink#nodeAttributeRemoved(java.lang.String
	 * , long, java.lang.String, java.lang.String)
	 */
	public void nodeAttributeRemoved(String sourceId, long timeId,
			String nodeId, String attribute) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;

		Node n = getDBNode(nodeId);
		Transaction t = graphDb.beginTx();

		try {
			n.removeProperty(attribute);
			t.success();
		} catch (Throwable e) {
			t.failure();
		} finally {
			t.finish();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.graphstream.stream.AttributeSink#edgeAttributeAdded(java.lang.String,
	 * long, java.lang.String, java.lang.String, java.lang.Object)
	 */
	public void edgeAttributeAdded(String sourceId, long timeId, String edgeId,
			String attribute, Object value) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;

		Relationship r = getDBEdge(edgeId);
		Transaction t = graphDb.beginTx();

		try {
			r.setProperty(attribute, value);
			t.success();
		} catch (Throwable e) {
			t.failure();
		} finally {
			t.finish();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.graphstream.stream.AttributeSink#edgeAttributeChanged(java.lang.String
	 * , long, java.lang.String, java.lang.String, java.lang.Object,
	 * java.lang.Object)
	 */
	public void edgeAttributeChanged(String sourceId, long timeId,
			String edgeId, String attribute, Object oldValue, Object newValue) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;

		Relationship r = getDBEdge(edgeId);
		Transaction t = graphDb.beginTx();

		try {
			r.setProperty(attribute, newValue);
			t.success();
		} catch (Throwable e) {
			t.failure();
		} finally {
			t.finish();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.graphstream.stream.AttributeSink#edgeAttributeRemoved(java.lang.String
	 * , long, java.lang.String, java.lang.String)
	 */
	public void edgeAttributeRemoved(String sourceId, long timeId,
			String edgeId, String attribute) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;

		Relationship r = getDBEdge(edgeId);
		Transaction t = graphDb.beginTx();

		try {
			r.removeProperty(attribute);
			t.success();
		} catch (Throwable e) {
			t.failure();
		} finally {
			t.finish();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.stream.ElementSink#nodeAdded(java.lang.String, long,
	 * java.lang.String)
	 */
	public void nodeAdded(String sourceId, long timeId, String nodeId) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;

		Transaction tx = graphDb.beginTx();

		try {
			Node n = graphDb.createNode();
			n.setProperty("id", nodeId);

			tx.success();
		} catch (Throwable t) {
			tx.failure();
		} finally {
			tx.finish();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.stream.ElementSink#nodeRemoved(java.lang.String,
	 * long, java.lang.String)
	 */
	public void nodeRemoved(String sourceId, long timeId, String nodeId) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;

		Node n = getDBNode(nodeId);

		if (n == null) {
			System.err.printf("unknown node '%s'\n", nodeId);
			return;
		}

		String req = "START node(" + n.getId() + ") MATCH n-[r]-() DELETE n, r";
		executeCypher(req);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.stream.ElementSink#edgeAdded(java.lang.String, long,
	 * java.lang.String, java.lang.String, java.lang.String, boolean)
	 */
	public void edgeAdded(String sourceId, long timeId, String edgeId,
			String fromNodeId, String toNodeId, boolean directed) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;

		Node src = getDBNode(fromNodeId);
		Node trg = getDBNode(toNodeId);
		Transaction tx = graphDb.beginTx();

		try {
			Relationship r = src.createRelationshipTo(trg,
					directed ? Neo4JRelationshipType.DIRECTED
							: Neo4JRelationshipType.UNDIRECTED);
			r.setProperty("id", edgeId);

			tx.success();
		} catch (Throwable t) {
			tx.failure();
		} finally {
			tx.finish();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.stream.ElementSink#edgeRemoved(java.lang.String,
	 * long, java.lang.String)
	 */
	public void edgeRemoved(String sourceId, long timeId, String edgeId) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;

		Relationship r = getDBEdge(edgeId);
		Transaction tx = graphDb.beginTx();

		try {
			r.delete();
			tx.success();
		} catch (Throwable t) {
			tx.failure();
		} finally {
			tx.finish();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.stream.ElementSink#graphCleared(java.lang.String,
	 * long)
	 */
	public void graphCleared(String sourceId, long timeId) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;

		String req = "START n=node(*), r=rel(*) DELETE n, r";
		executeCypher(req);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.stream.ElementSink#stepBegins(java.lang.String,
	 * long, double)
	 */
	public void stepBegins(String sourceId, long timeId, double step) {
		if (mode == Mode.READ_ONLY || !sinkTime.isNewEvent(sourceId, timeId))
			return;
	}

	private class NodeCache extends Cache<String, Node> {
		public NodeCache(int capacity) {
			super(capacity);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.graphstream.neo4j.tools.Cache#createObject(java.lang.Object)
		 */
		protected Node createObject(String key) throws CacheCreationException {
			String req = "START n=node(*) WHERE n.id! = '" + key + "' RETURN n";
			ExecutionResult result = executeCypher(req);

			Iterator<Node> it = result.columnAs("n");

			if (!it.hasNext() && key.matches("^\\d+$")) {
				result = executeCypher("START n=node(" + key + ") RETURN n");
				it = result.columnAs("n");
			}

			if (!it.hasNext())
				throw new CacheCreationException("Node '%s' not found", key);
			else
				return it.next();
		}
	}

	private class EdgeCache extends Cache<String, Relationship> {
		public EdgeCache(int capacity) {
			super(capacity);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.graphstream.neo4j.tools.Cache#createObject(java.lang.Object)
		 */
		protected Relationship createObject(String key)
				throws CacheCreationException {
			String req = "START r=rel(*) " + "WHERE r.id! = '" + key + "' "
					+ "RETURN r";
			ExecutionResult result = executeCypher(req);

			Iterator<Relationship> it = result.columnAs("r");

			if (!it.hasNext() && key.matches("^\\d+$")) {
				result = executeCypher("START r=rel(" + key + ") RETURN r");
				it = result.columnAs("r");
			}

			if (!it.hasNext())
				throw new CacheCreationException("Relationship '%s' not found",
						key);
			else
				return it.next();
		}
	}

	public static void main(String... args) throws Exception {
		Neo4JProxy src = new Neo4JProxy();

		src.connect("dataset/twitter", Mode.READ_ONLY);

		String nodeId = "128671153010118656"; // 128671153010118656
		String edgeId = "6603";
		Node n;
		Relationship r;
		long m1 = System.currentTimeMillis();
		n = src.getDBNode(nodeId);
		long m2 = System.currentTimeMillis();
		System.out.printf("#1 in %dms\n", m2 - m1);
		System.out.println(n);

		m1 = System.currentTimeMillis();
		n = src.getDBNode(nodeId);
		m2 = System.currentTimeMillis();
		System.out.printf("#2 in %dms\n", m2 - m1);
		System.out.println(n.getClass());

		m1 = System.currentTimeMillis();
		r = src.getDBEdge(edgeId);
		m2 = System.currentTimeMillis();
		System.out.printf("#1 in %dms\n", m2 - m1);
		System.out.println(r);

		m1 = System.currentTimeMillis();
		r = src.getDBEdge(edgeId);
		m2 = System.currentTimeMillis();
		System.out.printf("#1 in %dms\n", m2 - m1);
		System.out.println(r);

		Thread[] tarray = new Thread[Thread.activeCount()];
		Thread.enumerate(tarray);

		for (Thread t : tarray)
			System.out.printf("Thread : %s\n", t.getName());

		src.disconnect();
	}
}
