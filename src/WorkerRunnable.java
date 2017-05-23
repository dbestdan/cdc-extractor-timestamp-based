import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WorkerRunnable implements Runnable, Config {
	public static Long uptodate = 0L;
	private BlockingQueue<Task> queue = null;
	// private FileOutputStream fop = null;
	// private File file;
	private Writer out = null;
	private Map<Timestamp, HashSet<Long>> map = null;
	// private File changedDataRecordCSvFile = null;
	// private PrintWriter changedDataRecordFileWriter = null;
	private Long totalWaitTime = 0L;
	private String threadName = "";
	private String tableInitial = "";
	private String tableName = "";
	private long sessionNextTimeStamp = 0;
	private long sessionEndTime = 0L;
	private long taskCount = 0L;
	private long taskCountPerMinute = 0L;
	private long rowCountPerMinute = 0L;
	private long averageRowCountPerMinute = 0L;
	private long rowCount = 0L;
	private long averageRowCount = 0L;
	private CoordinatorRunnable parent = null;
	private long taskProcessingTime = 0L;
	private ResultSetMetaData rsmd = null;
	private int columnCount = 0;
	

	public WorkerRunnable(String threadName, String tableName, String tableInitial, CoordinatorRunnable parent,
			long sessionEndTime) {
		this.threadName = threadName;
		this.tableName = tableName;
		this.tableInitial = tableInitial;
		this.parent = parent;
		this.queue = parent.getQueue();
		this.sessionEndTime = sessionEndTime;
		this.map = new HashMap<Timestamp, HashSet<Long>>();
		Date date = null;
		try {
			date = dateFormat.parse("2010-10-10-10-10-10");
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		long time = date.getTime();

		String fileName = "chunk_" + threadName;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName, true), "UTF-8"));
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			e.printStackTrace();
		}
		this.totalWaitTime = 0L;

	}

	@Override
	public void run() {

		Connection conn = Client.getConnection();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String query = "select * from " + tableName + " where  " + tableInitial + "_modified_time >= ? and "
					+ tableInitial + "_modified_time < ? ";

			stmt = conn.prepareStatement(query);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		try {

			sessionNextTimeStamp = System.currentTimeMillis() + 60000;
			while (sessionEndTime > System.currentTimeMillis()) {
				long startWaitTime = System.nanoTime();
				long startWaitTimeMili = System.currentTimeMillis();
				Task task = queue.take();

				// keep track of task
				taskCount++;
				taskCountPerMinute++;

				stmt.setLong(1, task.getStartTime());
				stmt.setLong(2, task.getEndTime());
				rs = stmt.executeQuery();
				rsmd= rs.getMetaData();
				columnCount = rsmd.getColumnCount();

				while (rs.next()) {
					// keep track of number of rows
					rowCountPerMinute++;
					rowCount++;

					// write to a file
					writeLocalFile(rs);

				}

				long tmp = task.getEndTime();
				synchronized (uptodate) {
					if (uptodate == 0 || uptodate < tmp) {
						uptodate = tmp;
					}
				}

			}
			System.out.println("thread exit: " + threadName);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
				out.close();
			} catch (SQLException | IOException e) {
				e.printStackTrace();
			}
		}

	}

	private void writeLocalFile(ResultSet rs) {

		try {
			StringBuffer sb = new StringBuffer();
			sb.append(tableName + "|");
			for (int i = 1; i < columnCount; i++) {
				sb.append(rs.getString(i) + "|");
			}
			sb.append("\n");
			out.append(sb.toString());
			out.flush();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}

	}

}
