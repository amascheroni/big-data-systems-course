package com.bigdata;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * @author Maximiliano Agustin Mascheroni
 *
 */
public class ReadZNode {

	private static ZooKeeper zookeeper;
	private static ZooKeeperConnector connector;

	public static byte[] read(String path) throws KeeperException, InterruptedException {
		boolean watch = true;
		return zookeeper.getData(path, watch, zookeeper.exists(path, watch));
	}

	public static void main(String[] args)
			throws KeeperException, InterruptedException, IllegalStateException, IOException {
		String path = "/sampleznode";
		byte[] data;

		connector = new ZooKeeperConnector();
		zookeeper = connector.connect("localhost");
		data = read(path);

		// Print ASCII Value for the data
		for (byte b : data) {
			System.out.print(b);
		}

		System.out.println("\n");
		
		// Print real value
		for (byte b : data) {
			System.out.print((char) b);
		}
	}

}
