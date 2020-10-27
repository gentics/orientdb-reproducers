package com.gentics.odb;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
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

	/**
	 * Test parameters
	 */
	private boolean reUseVertex = false;

	public static final int INITIAL_TEXT_SIZE = 30;

	/**
	 * Reduce the property size by two paragraphs for replaced vertices
	 */
	public static final int REDUCTION_STEP = 0;

	public static final int VERTEX_COUNT = 5_000;

	public static final int DELETE_CREATE_OPS = 50_000;

	private List<RecordInfo> ids = new ArrayList<>(VERTEX_COUNT);

	private static final File DB_FOLDER = new File("target", DB_NAME);
	private static final File WAL_FOLDER = new File("target", "wal");

	@Before
	public void setupDB() {
		try {
			FileUtils.deleteDirectory(DB_FOLDER);
			FileUtils.deleteDirectory(WAL_FOLDER);
		} catch (IOException e) {
			e.printStackTrace();
		}
		assertTrue("WAL Folder could not be created.", WAL_FOLDER.mkdirs());
		// OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.setValue("gzip");
		// OGlobalConfiguration.WAL_CACHE_SIZE.setValue(0);
		//OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE.setValue(1);
		// OGlobalConfiguration.WAL_MAX_SIZE.setValue(20);
		OGlobalConfiguration.WAL_LOCATION.setValue(WAL_FOLDER.getAbsolutePath());

		factory = new OrientGraphFactory("plocal:" + DB_FOLDER.getAbsolutePath()).setupPool(16, 100);
		addTypes();
	}

	private void addTypes() {
		addVertexType(factory::getNoTx, CONTENT_TYPE, null, null);
	}

	@Test
	public void testFragmentation() throws InterruptedException {
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

		System.out.println("\nBefore:");
		long initialSize = printDBSize();
		System.out.println();

		long deletedRecords = reUseVertex ? reUseVertices() : replaceVertices();

		Orient.instance().shutdown();
		Thread.sleep(5000);
		System.out.println("\nFinal result:");
		long finalSize = printDBSize();
		long expectedTombstoneSize = deletedRecords * 11;
		long effSize = finalSize - expectedTombstoneSize;
		double factor = (double) effSize / (double) initialSize;

		System.out.println();
		System.out.println("DB increased by " + toHumanSize(finalSize - initialSize) + " factor: " + String.format("%1.2f", factor));
		System.out.println("Expected tombstone size: " + toHumanSize(expectedTombstoneSize));

	}

	private long replaceVertices() {

		// Randomly delete and create new records / elements
		System.out
			.println("Replace Vertices: Now invoking " + DELETE_CREATE_OPS + " delete & create operations.");
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
				printDBSize();
				// System.out.println("Delete / Create in Tx: " + i + " size: " + (currentSize / 1024 / 1024) + " MB");
			}
		}
		return deletedRecords;
	}

	private long reUseVertices() {
		// Randomly delete and create new records / elements
		System.out.println("Re-Use Vertices: Now invoking " + DELETE_CREATE_OPS + " update operations with smaller text size for updated records");
		for (int i = 0; i < DELETE_CREATE_OPS; i++) {
			RecordInfo info = getRandomRecord();
			OrientGraph tx = factory.getTx();

			try {
				// 1. Delete the found record
				OrientVertex v = tx.getVertex(info.id);
				clearVertex(v);
				ids.remove(info);

				// 2. Reduce the text size for the new record and create it
				int size = info.textSize - REDUCTION_STEP;
				v.setProperty("text", lorem.getParagraphs(size, size));
				ids.add(new RecordInfo(v.getId(), size));
				tx.commit();
			} finally {
				tx.shutdown();
			}
			if (i % 5000 == 0) {
				printDBSize();
				// System.out.println("Update in Tx: " + i + " size: " + (currentSize / 1024 / 1024) + " MB");
			}
		}
		return 0;
	}

	private void clearVertex(OrientVertex v) {
		for (String key : v.getPropertyKeys()) {
			v.removeProperty(key);
		}
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

	private long printDBSize() {
		File dbFolder = new File("target", DB_NAME);

		long pclSize = 0;
		long cpmSize = 0;
		long walSize = FileUtils.sizeOfDirectory(WAL_FOLDER);
		long other = 0;
		for (File file : dbFolder.listFiles()) {
			long size = file.length();
			String ext = FilenameUtils.getExtension(file.getName()).toLowerCase();
			switch (ext) {
			case "pcl":
				pclSize += size;
				break;
			case "cpm":
				cpmSize += size;
				break;
			default:
				other += size;
				break;
			}
		}

		System.out.println(
			"WAL: " + toHumanSize(walSize) + ", PCL: " + toHumanSize(pclSize) + ", CPM: " + toHumanSize(cpmSize) + ", Other: " + toHumanSize(other));
		return pclSize + cpmSize + other;
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
