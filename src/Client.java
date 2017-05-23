import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.ErrorManager;

public class Client {

	public static void main(String[] args){
		ArrayList<Thread> threads = new ArrayList<Thread>();
		long runDuration = Long.parseLong(System.getProperty("runDuration"))*60000L;
		long sessionEndTime = System.currentTimeMillis()+runDuration;
		System.out.println("run Duration ="+ runDuration);
		
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(System.getProperty("prop")));
		}catch(IOException e) {
			System.out.println("CDC-Extractor-Time-based: Could not load property file");
		}
		
		String clientName = System.getProperty("client");
		String clientTables ="";
		
		
		if(clientName.equals("client1")){
			clientTables = prop.getProperty("client1");
				}
		else if(clientName.equals("client2")) {
			clientTables = prop.getProperty("client2");
		}
		
		String[] clientTablesArray = clientTables.split(",");
		
		for(int i=0; i<clientTablesArray.length-1;i++) {
			String tableNameAndThreadSize = clientTablesArray[i];
			String[] tableNameAndThreadSizeArray = tableNameAndThreadSize.split("-");
			String tableName = tableNameAndThreadSizeArray[0];
			String tableInitial = tableNameAndThreadSizeArray[1];
			int threadSize = Integer.parseInt(tableNameAndThreadSizeArray[2]);
			
			CoordinatorRunnable coordinator = new CoordinatorRunnable(tableName, sessionEndTime);
			threads.add(new Thread(coordinator));
			//for worker thread of each table
			for(int j = 0; j<threadSize; j++) {
				WorkerRunnable worker = new WorkerRunnable(tableName+j, tableName, tableInitial, coordinator, sessionEndTime);
				threads.add(new Thread(worker));
			}
		}
		
		QueryRequestRunnable queryRequestRunnable= new QueryRequestRunnable();
		threads.add(new Thread(queryRequestRunnable));
		
	
		for(int i=0; i<threads.size(); i++){
			threads.get(i).start();
		}
		try {
			Thread.sleep(runDuration);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		System.exit(0);
		
		

		
		
		
	}
	
	public static Connection getConnection(){
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", "benchmarksql");
		connectionProps.put("password", "benchmarksql");
		try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager.getConnection("jdbc:postgresql://pcnode2:5432/benchmarksql", connectionProps);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
}
