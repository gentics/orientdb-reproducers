package com.gentics.odb;

import org.junit.Test;

import com.gentics.odb.server.Database;

public class OpenImportedTest extends AbstractOrientTest {

	@Test
	public void testStartServer() throws Exception {
		Database server = new Database("NodeA", ImportTest.STORAGE_PATH, "2481-2481", "2425-2425");
		server.startOrientServer(true);
	}

}
