package com.bigdata;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * @author Maximiliano Agustin Mascheroni
 *
 */
public class DeleteZNode {

	private ZooKeeper zookeeper;

	public DeleteZNode(ZooKeeper zookeeper) {
		this.zookeeper = zookeeper;
	}

	/**
	 * Deletes a ZNode
	 * 
	 * @param path
	 * @throws KeeperException
	 * @throws InterruptedException
	 * 
	 *  @author Maximiliano Agustin Mascheroni
	 */
	public void delete(String path, boolean watch) throws KeeperException, InterruptedException {
		System.out.println("\nDeleting ZNode \"" + path + "\"...");
		int version = zookeeper.exists(path, watch).getVersion();
		zookeeper.delete(path, version);
		System.out.println("[SUCCESS]: \"" + path + "\" ZNode was deleted");
	}

	/**
	 * Deletes a ZNode
	 * 
	 * @param path
	 * @param watch
	 * @param version
	 * @throws KeeperException
	 * @throws InterruptedException
	 * 
	 * @author Maximiliano Agustin Mascheroni
	 */
	public void delete(String path, boolean watch, int version) throws KeeperException, InterruptedException {
		System.out.println("\nDeleting ZNode \"" + path + "\"...");
		zookeeper.delete(path, version);
		System.out.println("[SUCCESS]: \"" + path + "\" ZNode was deleted");
	}

	public ZooKeeper getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(ZooKeeper zookeeper) {
		this.zookeeper = zookeeper;
	}

}
