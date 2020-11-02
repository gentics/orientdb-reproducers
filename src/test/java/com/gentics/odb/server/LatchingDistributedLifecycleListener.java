package com.gentics.odb.server;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.orientechnologies.orient.server.distributed.ODistributedLifecycleListener;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.DB_STATUS;

public class LatchingDistributedLifecycleListener implements ODistributedLifecycleListener {

	private CountDownLatch nodeJoinLatch = new CountDownLatch(1);

	private String selfNodeName;

	public LatchingDistributedLifecycleListener(String selfNodeName) {
		this.selfNodeName = selfNodeName;
	}

	@Override
	public boolean onNodeJoining(String iNode) {
		// Lock lock = hz.getLock("TX_LOCK");
		// lock.lock();
		return false;
	}

	@Override
	public void onNodeJoined(String iNode) {
		// Lock lock = hz.getLock("TX_LOCK");
		// lock.unlock();

	}

	@Override
	public void onNodeLeft(String iNode) {
	}

	@Override
	public void onDatabaseChangeStatus(String iNode, String iDatabaseName, DB_STATUS iNewStatus) {

		if ("storage".equals(iDatabaseName) && iNewStatus == DB_STATUS.ONLINE && iNode.equals(selfNodeName)) {
			System.out.println("Database is now online on {" + iNode + "}");
			nodeJoinLatch.countDown();
		}
	}

	public boolean waitForMainGraphDB(int timeout, TimeUnit unit) throws InterruptedException {
		return nodeJoinLatch.await(timeout, unit);
	}

}
