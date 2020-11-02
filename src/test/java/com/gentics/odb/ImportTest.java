package com.gentics.odb;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class ImportTest extends AbstractOrientTest {

	/**
	 * Database which was created in OrientDB 3.0.34
	 */
	private String importFile = "exports/export_02-11-2020_10-32-17-606.json.gz";

	public static final String STORAGE_PATH = "target/import_storage";
	public static final String DB_PATH = STORAGE_PATH + "/imported";

	@Before
	public void setupFolders() throws IOException {
		File storageDir = new File(STORAGE_PATH);
		FileUtils.deleteDirectory(storageDir);
		storageDir.mkdirs();
	}

	@Test
	public void testImport() throws Exception {
		OrientGraphFactory factory = new OrientGraphFactory("plocal:" + DB_PATH).setupPool(16, 100);
		ODatabaseDocumentTx db = factory.getDatabase();
		try {
			OCommandOutputListener listener = new OCommandOutputListener() {
				@Override
				public void onMessage(String iText) {
					System.out.println(iText);
				}
			};
			ODatabaseImport databaseImport = new ODatabaseImport(db, importFile, listener);
			databaseImport.importDatabase();
			databaseImport.close();
		} finally {
			db.close();
		}

	}

}
