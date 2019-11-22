package Maven1.Maven1;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;

public class sqoop1 {
sqoop1() throws Exception{
	 ConnBean cb = new ConnBean("localhost", "manish","1234");
	    SSHExec ssh = SSHExec.getInstance(cb);          
	    ssh.connect();
	   CustomTask sqoopCommand2 = new ExecCommand("/usr/lib/sqoop/bin/sqoop export --connect jdbc:mysql://localhost/emp --username root --password root --export-dir hdfs:/mysqldata/part-m-00000 --table emp --input-fields-terminated-by ','");
	    ssh.exec(sqoopCommand2);
	    ssh.disconnect();
}
}
