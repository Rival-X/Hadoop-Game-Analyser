package Maven1.Maven1;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;

public class pigtest {
	pigtest() throws Exception{
		ConnBean cb = new ConnBean("localhost", "manish","1234");
	    SSHExec ssh = SSHExec.getInstance(cb);          
	    ssh.connect();
	   CustomTask sqoopCommand1 = new ExecCommand("/usr/local/Work/hadoop-3.1.0/bin/hadoop fs -rm -r /mydata");
	   ssh.exec(sqoopCommand1);
	    ssh.disconnect();
	}
}
