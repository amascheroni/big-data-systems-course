package com.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * @author Maximiliano Agustin Mascheroni
 *
 */
public class ZooKeeperConnection {

	private static ZooKeeper zooKeeper;
	private static ZooKeeperConnector connector;
	private static List<String> znodeList;

	public static void main(String[] args) throws IOException,
						InterruptedException, IllegalStateException, KeeperException {
		connector = new ZooKeeperConnector();
		zooKeeper = connector.connect("localhost");
		znodeList = new ArrayList<String>();
		znodeList = zooKeeper.getChildren("/", true);

		for (String znode: znodeList) {
			System.out.println(znode);
		}

	}

}
