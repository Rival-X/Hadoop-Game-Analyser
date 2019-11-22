package Maven1.Maven1;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import com.sun.xml.internal.fastinfoset.sax.Properties;
public class pig {
    pig() {
        try {
            PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
            runQuery(pigServer);
            //Properties props = new Properties();
           // props.setProperty("fs.default.name", "hdfs://localhost:9870");
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    public static void runQuery(PigServer pigServer) {
        try {
            pigServer.registerQuery("input1 = LOAD '/hosp/ign.csv' as (line:chararray);");
            pigServer.registerQuery("words = foreach input1 generate FLATTEN(TOKENIZE(line)) as word;");
            pigServer.registerQuery("word_groups = group words by word;");
            pigServer.registerQuery("word_count = foreach word_groups generate group, COUNT(words);");
            pigServer.registerQuery("ordered_word_count = order word_count by group desc;");
            pigServer.registerQuery("store ordered_word_count into '/mydata';");
            
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
