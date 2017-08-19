package com.bigdata;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * @author Maximiliano Agustin Mascheroni
 *
 */
public class UpdateZNode {

	private ZooKeeper zookeeper;

	public UpdateZNode(ZooKeeper zookeeper) {
		this.zookeeper = zookeeper;
	}

	/**
	 * Updates a ZNode
	 * 
	 * @param path
	 * @param data
	 * @param watch
	 * @throws KeeperException
	 * @throws InterruptedException
	 * 
	 * @author Maximiliano Agustin Mascheroni
	 */
	public void update(String path, byte[] data, boolean watch) throws KeeperException, InterruptedException {
		System.out.println("\nUpdating ZNode \"" + path + "\"...");
		int version = zookeeper.exists(path, watch).getVersion();
		zookeeper.setData(path, data, version);
		System.out.println("[SUCCESS]: \"" + path + "\" ZNode was updated");
	}

	/**
	 * 
	 * @param path
	 * @param data
	 * @param version
	 * @throws KeeperException
	 * @throws InterruptedException
	 * 
	 * @author Maximiliano Agustin Mascheroni
	 */
	public void update(String path, byte[] data, int version) throws KeeperException, InterruptedException {
		System.out.println("\nUpdating ZNode \"" + path + "\"...");
		zookeeper.setData(path, data, version);
		System.out.println("[SUCCESS]: \"" + path + "\" ZNode was updated");
	}

	/**
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 * 
	 * @author Maximiliano Agustin Mascheroni
	 */
	public void update(String path, byte[] data) throws KeeperException, InterruptedException {
		System.out.println("\nUpdating ZNode \"" + path + "\"...");
		boolean watch = true;
		int version = zookeeper.exists(path, watch).getVersion();
		zookeeper.setData(path, data, version);
		System.out.println("[SUCCESS]: \"" + path + "\" ZNode was updated");
	}

	public ZooKeeper getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(ZooKeeper zookeeper) {
		this.zookeeper = zookeeper;
	}

}
