package com.gentics.odb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.Iterator;

import org.apache.tinkerpop.gremlin.orientdb.OrientEdge;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class Tinkerpop3IndexRegressionTest {

	public static final String EDGE_LABEL = "HAS_TEST_EDGE";

	public static final String KEY_1 = "key_1";
	public static final String VALUE_1 = "value1";

	public static final String KEY_2 = "key_2";
	public static final String VALUE_2 = "value2";

	public static final String KEY_3 = "key_3";
	public static final String VALUE_3 = "value3";

	private OrientGraphFactory factory;

	@Before
	public void setupDB() {
		String url = "memory:tinkerpop" + System.currentTimeMillis();
		factory = new OrientGraphFactory(url);
		addTypesAndIndices();
	}

	private void addTypesAndIndices() {
		try (OrientGraph noTx = factory.getNoTx()) {
			OSchema schema = noTx.getRawDatabase().getMetadata().getSchema();
			{
				OClass edgeClass = schema.getClass("E");
				OClass type = schema.createClass(EDGE_LABEL, edgeClass);

				type.createProperty("out", OType.LINK);
				type.createProperty(KEY_1, OType.STRING);
				type.createProperty(KEY_2, OType.STRING);
				type.createProperty(KEY_3, OType.STRING);

				String indexName = ("e." + EDGE_LABEL + "_test").toLowerCase();
				ODocument meta = new ODocument().fields("ignoreNullValues", true);
				String fields[] = {"out", KEY_1, KEY_2, KEY_3};
				OIndex idx = type.createIndex(indexName, INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString(), null, meta, fields);
				assertNotNull("Index was not created.", idx);
			}
			OClass vertexClass = schema.getClass("V");
			schema.createClass("NodeImpl", vertexClass);
			schema.createClass("ContentImpl", vertexClass);
		}
	}

	Object vertexId;

	@Test
	public void testEdgeLookup() {
		try (OrientGraph tx = factory.getTx()) {
			Vertex v1 = tx.addVertex("class:NodeImpl");
			Vertex v2 = tx.addVertex("class:ContentImpl");
			vertexId = v1.id();

			Edge edge1 = v1.addEdge(EDGE_LABEL, v2);
			edge1.property(KEY_1, VALUE_1);
			edge1.property(KEY_2, VALUE_2);
			edge1.property(KEY_3, VALUE_3);

			Edge edge2 = v2.addEdge(EDGE_LABEL, v1);
			edge2.property(KEY_1, VALUE_1);
			edge2.property(KEY_2, VALUE_2);
			edge2.property(KEY_3, VALUE_3);

			edge2.remove();
			edge1.remove();

			assertIndex(tx);

			tx.commit();
		}
	}

	private void assertIndex(OrientGraph tx) {
		OIndexManager indexManager = tx.getRawDatabase().getMetadata().getIndexManager();
		OIndex index = indexManager.getIndex(("e." + EDGE_LABEL + "_test").toLowerCase());
		Object key = new OCompositeKey(vertexId, VALUE_1, VALUE_2, VALUE_3);
		Iterator<OrientEdge> it = tx.getIndexedEdges(index, Collections.singleton(key).iterator()).iterator();
		int edgeCount = 0;
		while (it.hasNext()) {
			Edge foundEdge = it.next();
			assertNotNull("The iterator indicated with hasNext a element would exist. But we got null.", foundEdge);
			assertEquals(foundEdge.property(KEY_1).value(), VALUE_1);
			assertEquals(foundEdge.property(KEY_2).value(), VALUE_2);
			assertEquals(foundEdge.property(KEY_3).value(), VALUE_3);
			edgeCount++;
		}
//		assertEquals("Expected 1 edge to be found", 1, edgeCount);
		assertEquals("The index should not have returned edges", 0, edgeCount);
	}
}
