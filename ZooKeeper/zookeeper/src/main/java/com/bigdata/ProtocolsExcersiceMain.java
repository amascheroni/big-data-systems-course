package com.bigdata;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * ProtocolsExcersiceMain class
 * 
 * This class demonstrates the 3 protocols: create, update, delete Znodes
 * 
 * @author Maximiliano Agustin Mascheroni
 *
 */
public class ProtocolsExcersiceMain {

	private static final String HOST = "localhost";
	private static final String SAMPLE_NODE = "sampleznode";
	private static final String SAMPLE_NODE_PATH = "/" + SAMPLE_NODE;
	
	public static void main(String[] args) throws IllegalStateException, IOException, InterruptedException, KeeperException {
		
		//Connecting to Zookeeper
		ZooKeeperConnector connector = new ZooKeeperConnector(); 
		ZooKeeper zooKeeper = connector.connect(HOST);
		System.out.println("\n ==== Running create, update, delete protocols ==== \n");
		
		//List all of the ZNodes before starting
		System.out.println("Current Znodes: ");
		List<String> znodeList = zooKeeper.getChildren("/", true);
		for (String znode: znodeList) {
			System.out.println(znode);
		}

		//Creating a ZNode
		byte[] zNodeToAddData = "sample znode data for protocol 1 implementation".getBytes();
		CreateZNode createProtocol = new CreateZNode(zooKeeper);
		CreateMode mode = CreateMode.PERSISTENT;
		createProtocol.create(SAMPLE_NODE_PATH, zNodeToAddData, mode);
		
		//List all of the Znodes (including the new one) and their content
		boolean watch = true;
		list(zooKeeper, watch);

		//Updating the data of the added ZNode
		byte[] dataToUpdate = "I have been modified by the master".getBytes();
		int versionUpd = zooKeeper.exists(SAMPLE_NODE_PATH, watch).getVersion();
		UpdateZNode updateProtocol = new UpdateZNode(zooKeeper);
		updateProtocol.update(SAMPLE_NODE_PATH, dataToUpdate, versionUpd);
		
		//List all of the Znodes and their content again
		list(zooKeeper, watch);
		
		//Deleting the created ZNode
		int versionDel = zooKeeper.exists(SAMPLE_NODE_PATH, watch).getVersion();
		DeleteZNode deleteProtocol = new DeleteZNode(zooKeeper);
		deleteProtocol.delete(SAMPLE_NODE_PATH, watch, versionDel);

		//List ZNodes and data one more time
		list(zooKeeper, watch);
		System.out.println("");
		
		//Closing connection
		zooKeeper.close();
	}
	
	/**
	 * This method lists all of the Znodes and their content
	 * 
	 * @param ZooKeeper
	 * @param boolean
	 * @throws KeeperException
	 * @throws InterruptedException
	 * 
	 * @author Maximiliano Agustin Mascheroni
	 */
	private static void list(ZooKeeper zooKeeper, boolean watch) throws KeeperException, InterruptedException {
		System.out.println("\n Listing Znodes and their Data");
		List<String> newZNodeList = zooKeeper.getChildren("/", true);
		for (String znode: newZNodeList) {
			String path = "/" + znode;
			byte[] readData = zooKeeper.getData("/" + znode, watch, zooKeeper.exists(path, watch));
			System.out.print("Znode: " + znode + " - Data: ");
			for (byte b : readData) {
				System.out.print((char) b);
			}
			System.out.println("");
		}
	}
}
