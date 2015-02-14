package net.juniper.iq.stream;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SimpleClient {
	private Cluster cluster;
	private Session session;

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect();
	}

	public void close() {
		cluster.close();
	}
	
	public void loadData() { 
		long currenTime = System.currentTimeMillis();
//		session.execute(
//			      "INSERT INTO JunosIQStreamDB.metrics_ts_withmap_data (key, time, agg_values) " +
//			      "VALUES ('testkey'," + currenTime + ", {'min' : 5,'max' : 98, 'avg' : 45});");		
		
		
		session.execute(
			      "INSERT INTO testkeyspace.TestTable (key, time, value) " +
			      "VALUES ('testkey'," + currenTime + "," + 5 + ")");	
		
	}
	
/*	public void queryData() { 
		ResultSet results = session.execute("select * from JunosIQStreamDB.metrics_ts_withmap_data;");
		System.out.println("ROW...");
		for (Row row : results) {
			System.out.println(row);

		}		
	}*/
	
	public void queryData() { 
		ResultSet results = session.execute("select * from testkeyspace.TestTable;");
		System.out.println("ROW...");
		for (Row row : results) {
			System.out.println(row);

		}		
	}

	public static void main(String[] args) {
		SimpleClient client = new SimpleClient();
		client.connect("localhost");
		System.out.println("Connected...");
		
	    //client.loadData();
	    
	    client.queryData();
	    
		client.close();
		System.out.println("Closed...");
	}

}
