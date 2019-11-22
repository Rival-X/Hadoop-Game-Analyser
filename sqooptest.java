package Maven1.Maven1;

//import org.apache.sqoop.SqoopOptions;
//import org.apache.sqoop.tool.ImportTool;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.tool.ImportTool;
public class sqooptest {
	public static void main(String args[]) throws Exception{
		SqoopOptions options = new SqoopOptions();
        options.setConnectString("jdbc:mysql://HOSTNAME:PORT/DATABASE_NAME");
        //options.setTableName("TABLE_NAME");
        //options.setWhereClause("id>10");     // this where clause works when importing whole table, ie when setTableName() is used
        options.setUsername("USERNAME");
        options.setPassword("PASSWORD");
        //options.setDirectMode(true);    // Make sure the direct mode is off when importing data to HBase
        options.setNumMappers(8);         // Default value is 4
        options.setSqlQuery("SELECT * FROM user_logs WHERE $CONDITIONS limit 10");
        options.setSplitByCol("log_id");

        // HBase options
        options.setHBaseTable("HBASE_TABLE_NAME");
        options.setHBaseColFamily("colFamily");
        options.setCreateHBaseTable(true);    // Create HBase table, if it does not exist
        options.setHBaseRowKeyColumn("log_id");

        int ret = new ImportTool().run( options);
	}

}
