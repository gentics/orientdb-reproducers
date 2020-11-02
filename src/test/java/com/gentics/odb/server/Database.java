package com.gentics.odb.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerHandlerConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.DB_STATUS;
import com.orientechnologies.orient.server.plugin.OServerPluginManager;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class Database {

	public static final String TX_LOCK_KEY = "TX_LOCK";

	private String nodeName;
	private String basePath;
	private OServer server;
	private OrientGraphFactory factory;
	private String httpPort;
	private String binPort;
	private LatchingDistributedLifecycleListener listener;
	// private HazelcastInstance hazelcastInstance;

	public Database(String nodeName, String basePath, String httpPort, String binPort) {
		this.nodeName = nodeName;
		this.basePath = basePath;
		this.httpPort = httpPort;
		this.binPort = binPort;
	}

	public OServer getServer() {
		return server;
	}

	private String getOrientServerConfig() throws IOException {
		InputStream configIns = getClass().getResourceAsStream("/config/orientdb-server-config.xml");
		StringWriter writer = new StringWriter();
		IOUtils.copy(configIns, writer, StandardCharsets.UTF_8);
		String configString = writer.toString();
		System.setProperty("PORT_CONFIG_HTTP", httpPort);
		System.setProperty("PORT_CONFIG_BIN", binPort);
		System.setProperty("ORIENTDB_PLUGIN_DIR", "orient-plugins");
		System.setProperty("plugin.directory", "plugins");
		System.setProperty("ORIENTDB_CONFDIR_NAME", "config");
		System.setProperty("ORIENTDB_CONFDIR_PATH", new File("config").getAbsolutePath());
		System.setProperty("ORIENTDB_NODE_NAME", nodeName);
		System.setProperty("ORIENTDB_DISTRIBUTED", "true");
		System.setProperty("ORIENTDB_DB_PATH", escapePath(basePath));
		System.setProperty("ORIENTDB_HOME", new File(".").getAbsolutePath());
		configString = PropertyUtil.resolve(configString);
		return configString;
	}

	private String escapePath(String path) {
		return StringEscapeUtils.escapeJava(StringEscapeUtils.escapeXml11(new File(path).getAbsolutePath()));
	}

	public OServer startOrientServer(boolean waitForDB) throws Exception {

		String orientdbHome = new File("").getAbsolutePath();
		System.setProperty("ORIENTDB_HOME", orientdbHome);
		if (server == null) {
			this.server = OServerMain.create();
		}
		server.startup(getOrientServerConfig());
		startHazelcast();

		// ILock lock = hazelcastInstance.getLock(TX_LOCK_KEY);
		// lock.lock();
		try {
			OServerPluginManager manager = new OServerPluginManager();
			manager.config(server);
			server.activate();
			ODistributedServerManager distributedManager = server.getDistributedManager();
			this.listener = new LatchingDistributedLifecycleListener(nodeName);
			distributedManager.registerLifecycleListener(listener);

			manager.startup();
			postStartupDBEventHandling();
			System.out.println("Server startup done");

			// Replication may occur directly or we need to wait.
			if (waitForDB) {
				waitForDB();
			}

		} finally {
			System.out.println("Releasing lock");
			// lock.unlock();
		}
		return server;
	}

	public void startHazelcast() throws FileNotFoundException {
		// hazelcastInstance = CustomOHazelcastPlugin.createHazelcast(hazelcastPluginConfig.parameters);
	}

	public void addEdgeType(Supplier<OrientGraphNoTx> txProvider, String label, String superTypeName) {
		System.out.println("Adding edge type for label {" + label + "}");

		OrientGraphNoTx noTx = txProvider.get();
		try {
			OrientEdgeType edgeType = noTx.getEdgeType(label);
			if (edgeType == null) {
				String superClazz = "E";
				if (superTypeName != null) {
					superClazz = superTypeName;
				}
				edgeType = noTx.createEdgeType(label, superClazz);

				// Add index
				String fieldKey = "name";
				edgeType.createProperty(fieldKey, OType.STRING);
				boolean unique = false;
				String indexName = label + "_name";
				edgeType.createIndex(indexName.toLowerCase(),
					unique ? OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.toString() : OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString(),
					null, new ODocument().fields("ignoreNullValues", true), new String[] { fieldKey });
			}
		} finally {
			noTx.shutdown();
		}

	}

	public void addVertexType(Supplier<OrientGraphNoTx> txProvider, String typeName, String superTypeName,
		Consumer<OrientVertexType> typeModifier) {

		System.out.println("Adding vertex type for class {" + typeName + "}");

		OrientGraphNoTx noTx = txProvider.get();
		try {
			OrientVertexType vertexType = noTx.getVertexType(typeName);
			if (vertexType == null) {
				String superClazz = "V";
				if (superTypeName != null) {
					superClazz = superTypeName;
				}
				vertexType = noTx.createVertexType(typeName, superClazz);

				if (typeModifier != null) {
					typeModifier.accept(vertexType);
				}
			}
		} finally {
			noTx.shutdown();
		}
	}

	private void postStartupDBEventHandling() {
		// Get the database status
		// DB_STATUS status = server.getDistributedManager().getDatabaseStatus(nodeName, "storage");
		// Pass it along to the topology event bridge
		// listener.onDatabaseChangeStatus(nodeName, "storage", status);
	}

	private void waitForDB() throws InterruptedException {
		System.out.println("Waiting for database");
		listener.waitForMainGraphDB(20, TimeUnit.SECONDS);
		System.out.println("Found database");
	}

	public OrientGraph getTx() {
		if (factory != null) {
			return factory.getTx();
		} else {
			ODatabaseSession db = server.getContext().open("storage", "admin", "admin");
			return (OrientGraph) OrientGraphFactory.getTxGraphImplFactory().getGraph((ODatabaseDocumentInternal) db);
		}
	}

	public OrientGraphNoTx getNoTx() {
		if (factory != null) {
			return factory.getNoTx();
		} else {
			ODatabaseSession db = server.getContext().open("storage", "admin", "admin");
			return (OrientGraphNoTx) OrientGraphFactory.getNoTxGraphImplFactory().getGraph((ODatabaseDocumentInternal) db);
		}
	}

	public void create(String name) {
		server.createDatabase(name, ODatabaseType.PLOCAL, OrientDBConfig.defaultConfig());
	}

	public void openLocally(String name) {
		File base = new File("/media/ext4/db", nodeName);
		factory = new OrientGraphFactory("plocal:" + new File(base, name).getAbsolutePath()).setupPool(16, 100);
	}

	public void close() {
		if (factory != null) {
			factory.close();
			factory = null;
		}
		if (server != null) {
			server.shutdown();
		}
	}

	public void backup() {
		DateFormat formatter = new SimpleDateFormat("dd-MM-yyyy_HH-mm-ss-SSS");
		String backupDirectory = "target/backups";
		ODatabaseSession db = server.getContext().open("storage", "admin", "admin");
		try {
			OCommandOutputListener listener = new OCommandOutputListener() {
				@Override
				public void onMessage(String iText) {
					System.out.println(iText);
				}
			};
			String dateString = formatter.format(new Date());
			String backupFile = "backup_" + dateString + ".zip";
			new File(backupDirectory).mkdirs();
			String absolutePath = new File(backupDirectory, backupFile).getAbsolutePath();
			try (OutputStream out = new FileOutputStream(absolutePath)) {
				db.backup(out, null, null, listener, 1, 2048);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} finally {
			db.close();
		}
	}

}
