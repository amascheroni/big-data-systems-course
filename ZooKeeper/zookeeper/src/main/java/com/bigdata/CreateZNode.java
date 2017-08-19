package com.bigdata;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * @author Maximiliano Agustin Mascheroni
 *
 */
public class CreateZNode {

	private ZooKeeper zooKeeper;

	public CreateZNode(ZooKeeper zooKeeper) {
		this.zooKeeper = zooKeeper;
	}

	/**
	 * Creates a ZNode
	 * 
	 * @param path
	 * @param data
	 * @param mode
	 * @throws KeeperException
	 * @throws InterruptedException
	 * 
	 * @author Maximiliano Agustin Mascheroni
	 */
	public void create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
		System.out.println("\nCreating ZNode \"" + path + "\"...");
		zooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
		System.out.println("[SUCCESS]: \"" + path + "\" ZNode was created");
	}

	/**
	 * Creates a ZNode quickly with Persistent Mode
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 * 
	 * @author Maximiliano Agustin Mascheroni
	 */
	public void createPersistent(String path, byte[] data) throws KeeperException, InterruptedException {
		System.out.println("\nCreating ZNode " + path + "...");
		zooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("[SUCCESS]: \"" + path + "\" ZNode was created");
	}

	public ZooKeeper getZooKeeper() {
		return zooKeeper;
	}

	public void setZooKeeper(ZooKeeper zooKeeper) {
		this.zooKeeper = zooKeeper;
	}

}
