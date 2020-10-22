package com.gentics.odb;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class StorageFragmentationTest extends AbstractOrientTest {

	private OrientGraphFactory factory;

	public static final String DB_NAME = StorageFragmentationTest.class.getSimpleName();

	public static final String CONTENT_TYPE = "ContentImpl";

	public static Lorem lorem = LoremIpsum.getInstance();

	public static final int INITIAL_TEXT_SIZE = 30;

	/**
	 * Reduce the property size by two paragraphs for replaced vertices
	 */
	public static final int REDUCTION_STEP = 2;

	public static final int VERTEX_COUNT = 5_000;

	public static final int DELETE_CREATE_OPS = 50_000;

	private List<RecordInfo> ids = new ArrayList<>(VERTEX_COUNT);

	@Before
	public void setupDB() {
		try {
			FileUtils.deleteDirectory(new File("target", DB_NAME));
		} catch (IOException e) {
			e.printStackTrace();
		}
		factory = new OrientGraphFactory("plocal:target/" + DB_NAME).setupPool(16, 100);
		addTypes();
	}

	private void addTypes() {
		addVertexType(factory::getNoTx, CONTENT_TYPE, null, null);
	}

	@Test
	public void testFragmentation() {
		// Add vertices which have a large record size
		System.out.println("Creating " + VERTEX_COUNT + " vertices with text size " + INITIAL_TEXT_SIZE);
		for (int i = 0; i < VERTEX_COUNT; i++) {
			OrientGraph tx = factory.getTx();
			try {
				Vertex v = addContent(tx, INITIAL_TEXT_SIZE);
				tx.commit();
				ids.add(new RecordInfo(v.getId(), INITIAL_TEXT_SIZE));
			} finally {
				tx.shutdown();
			}
			if (i % 1000 == 0) {
				System.out.println("Created " + i + " vertices");
			}
		}
		System.out.println();
		long initialSize = folderSize();

		// Randomly delete and create new records / elements
		System.out.println("Now invoking " + DELETE_CREATE_OPS + " delete & create operations with smaller text size for new records");
		long deletedRecords = 0;
		for (int i = 0; i < DELETE_CREATE_OPS; i++) {
			RecordInfo info = getRandomRecord();
			OrientGraph tx = factory.getTx();

			try {
				// 1. Delete the found record
				OrientVertex v = tx.getVertex(info.id);
				v.remove();
				deletedRecords++;
				ids.remove(info);

				// 2. Reduce the text size for the new record and create it
				int size = info.textSize - REDUCTION_STEP;
				Vertex added = addContent(tx, size);
				tx.commit();
				ids.add(new RecordInfo(added.getId(), size));
			} finally {
				tx.shutdown();
			}
			if (i % 5000 == 0) {
				long currentSize = folderSize();
				System.out.println("Delete / Create in Tx: " + i + " size: " + (currentSize / 1024 / 1024) + " MB");
			}
		}

		long finalSize = folderSize();
		long expectedTombstoneSize = deletedRecords * 11;
		long effSize = finalSize - expectedTombstoneSize;
		double factor = (double) effSize / (double) initialSize;
		System.out.println();
		System.out.println("DB increased by " + toHumanSize(finalSize - initialSize) + " factor: " + String.format("%1.2f", factor));
		System.out.println("Expected tombstone size: " + toHumanSize(expectedTombstoneSize));

	}

	/**
	 * Return a random record for which the property can be reduced in size.
	 * 
	 * @return
	 */
	private RecordInfo getRandomRecord() {
		int MAX_TRIES = 200;
		for (int i = 0; i < MAX_TRIES; i++) {
			int r = (int) (Math.random() * ids.size());
			RecordInfo info = ids.get(r);
			if (info.textSize - REDUCTION_STEP > 1) {
				return info;
			}
		}
		fail("Failed to find a record with a larger text size.");
		return null;
	}

	private long folderSize() {
		return FileUtils.sizeOfDirectory(new File("target", DB_NAME));
	}

	public String toHumanSize(long size) {
		if (size < 1024) {
			return size + " Bytes";
		} else if (size < 1024 * 1024) {
			return (size / 1024) + " KB";
		} else {
			return (size / 1024 / 1024) + " MB";
		}
	}

	private Vertex addContent(OrientGraph tx, int size) {
		OrientVertex v = tx.addVertex("class:" + CONTENT_TYPE);
		v.setProperty("text", lorem.getParagraphs(size, size));
		return v;
	}

	class RecordInfo {
		Object id;
		int textSize;

		public RecordInfo(Object id, int textSize) {
			this.id = id;
			this.textSize = textSize;
		}
	}

}
