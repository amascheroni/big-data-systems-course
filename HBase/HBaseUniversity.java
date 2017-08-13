package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUniversity {

	private static final String TABLE_NAME = "bigDataSystemsCourse";
	private static final String STUDENT_COLUMN = "studentData";
	private static final String PROFESSOR_COLUMN = "professorData";

	public static void main(String[] args) throws IOException {

		// Connecting to HMaster
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", "http://localhost:60010");

		HBaseAdmin admin = null;

		try {
			admin = new HBaseAdmin(config);
		} catch (MasterNotRunningException e) {
			System.out.println("Could not setup HBaseAdmin as no master is running. Start HBase First");
			System.exit(0);
		}

		if (!admin.tableExists(TABLE_NAME)) {

			// Creating table "bigDataSystemsCourse"
			admin.createTable(new HTableDescriptor(TABLE_NAME));

			// Disabling the table so we can make changes to it
			admin.disableTable(TABLE_NAME);

			// Adding two columns: "studentData" and "proffesorData"
			admin.addColumn(TABLE_NAME, new HColumnDescriptor(STUDENT_COLUMN));
			admin.addColumn(TABLE_NAME, new HColumnDescriptor(PROFESSOR_COLUMN));

			// Enabling the table for use
			admin.enableTable(TABLE_NAME);

		}

		// Get an instance of the table
		HTable table = new HTable(config, TABLE_NAME);

		// Adding first row called "student1"
		Put st = new Put(Bytes.toBytes("student1"));

		// Preparing name and lastname in "studentData" column to be stored
		st.add(Bytes.toBytes(STUDENT_COLUMN), Bytes.toBytes("name"), Bytes.toBytes("Maximiliano Agustin"));
		st.add(Bytes.toBytes(STUDENT_COLUMN), Bytes.toBytes("lastname"), Bytes.toBytes("Mascheroni"));

		// Pushing the changes made for "studentData"
		table.put(st);

		// Adding second row called "professor1"
		Put pr = new Put(Bytes.toBytes("professor1"));

		// Preparing name and lastname in "professorData" column to be stored
		pr.add(Bytes.toBytes(PROFESSOR_COLUMN), Bytes.toBytes("name"), Bytes.toBytes("Pramod"));
		pr.add(Bytes.toBytes(PROFESSOR_COLUMN), Bytes.toBytes("lastname"), Bytes.toBytes("Bhatotia"));

		// Pushing the changes made for "professorData"
		table.put(pr);

		// Getting the stored results for student using GET
		Get g = new Get(Bytes.toBytes("student1"));
		Result r = table.get(g);
		byte[] stName = r.getValue(Bytes.toBytes(STUDENT_COLUMN), Bytes.toBytes("name"));
		byte[] stLastName = r.getValue(Bytes.toBytes(STUDENT_COLUMN), Bytes.toBytes("lastname"));
		String valueName = Bytes.toString(stName);
		String valueLastName = Bytes.toString(stLastName);
		System.out.println("The student student1 is: " + valueName + " " + valueLastName);

		// Getting the stored results for professor by iterating (without knowing the row name)
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes(PROFESSOR_COLUMN), Bytes.toBytes("name"));
		s.addColumn(Bytes.toBytes(PROFESSOR_COLUMN), Bytes.toBytes("lastname"));
		ResultScanner scanner = table.getScanner(s);
		try {
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				String profName = Bytes.toString(rr.getValue(Bytes.toBytes(PROFESSOR_COLUMN),
						Bytes.toBytes("name")));
				String profLastName = Bytes.toString(rr.getValue(Bytes.toBytes(PROFESSOR_COLUMN),
						Bytes.toBytes("lastname")));
				String profRow = Bytes.toString(rr.getRow());
				System.out.println("The professor " + profRow + " is: " + profName + " " + profLastName);
			}
		} finally {
			scanner.close();
		}
		
		// Deleting the table after use
		admin.disableTable(TABLE_NAME);
		admin.deleteTable(TABLE_NAME);
		
		table.close();
	}
}
