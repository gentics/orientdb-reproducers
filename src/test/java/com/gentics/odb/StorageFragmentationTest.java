package com.gentics.odb;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class StorageFragmentationTest extends AbstractOrientTest {

	/* ************************************************************************
	 * Test settings
	 * ************************************************************************/
	private static final int INITIAL_TEXT_SIZE = (int) Math.ceil(0.4 * 1024 * 1024);
	private static final boolean REUSE_VERTEX = false;
	private static final double REDUCTION = .5;
	private static final boolean REDUCE_BY_MULTIPLICATION = true;
	private static final int VERTEX_COUNT = 5_000;
	private static final int DELETE_CREATE_OPS = 50_000;

	private static final String DB_NAME = StorageFragmentationTest.class.getSimpleName();
	private static final String CONTENT_TYPE = "ContentImpl";
	private static final File DB_FOLDER = new File("target", DB_NAME);
	private static final File WAL_FOLDER = new File("target", "wal");

	private final List<RecordInfo> ids = new ArrayList<>(VERTEX_COUNT);

	private OrientGraphFactory factory;
	private String content;


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

		System.out.printf(
			"Size will be reduced by %s %f",
			REDUCE_BY_MULTIPLICATION ? "multiplying with" : "subtracting",
			REDUCTION);

		System.out.println("\nBefore:");
		long initialSize = printDBSize();
		System.out.println();

		long deletedRecords = REUSE_VERTEX ? reUseVertices() : replaceVertices();

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
		System.out.println("Replace Vertices: Now invoking " + DELETE_CREATE_OPS + " delete & create operations.");
		long deletedRecords = 0;
		for (int i = 0; i < DELETE_CREATE_OPS; i++) {
			RecordInfo info = getRandomRecord();
			int size = reduceSize(info.textSize);
			OrientGraph tx = factory.getTx();
			try {
				// 1. Delete the found record
				OrientVertex v = tx.getVertex(info.id);
				v.remove();
				deletedRecords++;
				ids.remove(info);

				// 2. Reduce the text size for the new record and create it
				Vertex added = addContent(tx, size);
				tx.commit();
				ids.add(new RecordInfo(added.getId(), size));
			} finally {
				tx.shutdown();
			}
			if (i % 5000 == 0) {
				printDBSize();
			}
		}
		return deletedRecords;
	}

	private int reduceSize(int curSize) {
		if (REDUCE_BY_MULTIPLICATION) {
			return (int) Math.ceil(curSize * REDUCTION);
		}

		return Math.max(1, curSize - (int) REDUCTION);
	}

	private long reUseVertices() {
		// Randomly delete and create new records / elements
		System.out.println("Re-Use Vertices: Now invoking " + DELETE_CREATE_OPS + " update operations with smaller text size for updated records");
		for (int i = 0; i < DELETE_CREATE_OPS; i++) {
			RecordInfo info = getRandomRecord();
			int size = reduceSize(info.textSize);
			OrientGraph tx = factory.getTx();

			try {
				// 1. Delete the found record
				OrientVertex v = tx.getVertex(info.id);
				clearVertex(v);
				ids.remove(info);

				// 2. Reduce the text size for the new record and create it
				v.setProperty("text", getData(size));
				ids.add(new RecordInfo(v.getId(), size));
				tx.commit();
			} finally {
				tx.shutdown();
			}
			if (i % 5000 == 0) {
				printDBSize();
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
	 * Return a random record.
	 *
	 * @return A random record
	 */
	private RecordInfo getRandomRecord() {
		return ids.get((int) (Math.random() * ids.size()));
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

		System.out.printf(
			"WAL: %s, PCL: %s, CPM: %s, Other %s%n",
			toHumanSize(walSize),
			toHumanSize(pclSize),
			toHumanSize(cpmSize),
			toHumanSize(other));

		return pclSize + cpmSize + other;
	}

	public String toHumanSize(long size) {
		String unit;
		int mbFactor = 1024 * 1024;

		if (size < 1024) {
			unit = "Bytes";
		} else if (size < mbFactor) {
			unit = "KB";
			size /= 1024;
		} else {
			unit = "MB";
			size /= mbFactor;
		}

		return String.format("%5d %s", size, unit);
	}

	private Vertex addContent(OrientGraph tx, int size) {
		if (content == null) {
			content = RandomStringUtils.randomAlphanumeric(INITIAL_TEXT_SIZE);
		}
		OrientVertex v = tx.addVertex("class:" + CONTENT_TYPE);
		v.setProperty("text", getData(size));
		return v;
	}

	private String getData(int size) {
		if (content == null) {
			content = RandomStringUtils.randomAlphanumeric(INITIAL_TEXT_SIZE);
		}

		if (size > content.length()) {
			System.out.printf("WARNING: current size %d is greater than data length %d%n", size, content.length());
		}

		return content.substring(0, size);
	}

	static class RecordInfo {
		Object id;
		int textSize;

		public RecordInfo(Object id, int textSize) {
			this.id = id;
			this.textSize = textSize;
		}
	}

}
