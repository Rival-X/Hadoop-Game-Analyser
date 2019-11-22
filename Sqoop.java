package Maven1.Maven1;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;

public class Sqoop {
Sqoop() throws Exception{
	 ConnBean cb = new ConnBean("localhost", "manish","1234");
	    SSHExec ssh = SSHExec.getInstance(cb);          
	    ssh.connect();
	   CustomTask sqoopCommand2 = new ExecCommand("/usr/lib/sqoop/bin/sqoop import --connect jdbc:mysql://localhost/emp --username root --password root --table emp --m 1 --target-dir /mysqldata");
	    ssh.exec(sqoopCommand2);
	    ssh.disconnect();
}
}
