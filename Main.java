package Maven1.Maven1;

import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import javax.swing.JOptionPane;

public class Main
extends JFrame{

        public static final Component Frame = null;
		JTextArea textArea;
        String filename;
        JButton btn,btn1,btn2,btn3,btn4,btn5,btn6;
        Main() throws Exception{
            JPanel panel=new JPanel();
        btn=new JButton("View");
        btn1=new JButton("Merge");
        btn2=new JButton("Count");
            btn3=new JButton("PIG");
            btn4=new JButton("Sqoop");
            btn5=new JButton("Reset");
            btn6=new JButton("Spark");
            textArea=new JTextArea();
            JScrollPane js=new JScrollPane(textArea);
        btn.addActionListener(new View());
        btn1.addActionListener(new View1());
        btn2.addActionListener(new View2());
        btn3.addActionListener(new View3());
        btn4.addActionListener(new View4());
        btn5.addActionListener(new View5());
        btn6.addActionListener(new View6());
        panel.add(btn);
        panel.add(btn1);
        panel.add(btn2);
        panel.add(btn3);
        panel.add(btn4);
        panel.add(btn6);
        panel.add(btn5);
        getContentPane().add(panel,BorderLayout.NORTH);
      getContentPane().add(js);
    }
    public static void main(String[] args) throws Exception{
       JFrame Frame=new Main();

        Frame.setSize(600, 600);
        Frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        Frame.setVisible(true);
    }

    class View implements ActionListener{
        public void actionPerformed(ActionEvent e) {
        ReadFile("hdfs:/hosp1");
        }
        }
    void ReadFile(String pt)
    {
        //BufferedReader d;
        StringBuffer sb = new StringBuffer();
        try
        {
        	
    		Path t=new Path(pt);
			FileSystem fs=FileSystem.get(new Configuration());
			BufferedReader d=new BufferedReader(new InputStreamReader(fs.open(t)));
	
   		 	//FileSystem hdfs=FileSystem.get(conf);
            //d = new BufferedReader(new FileReader("hdfs:/hosp1"));
            String line;
            while((line=d.readLine())!=null)
                sb.append(line + "\n");
            textArea.setText(sb.toString());
            d.close();
        }
        catch(FileNotFoundException e)
        {
            System.out.println("File not Found1");
        }
        catch(IOException ioe){}
    }
    class View1 implements ActionListener{
		public void actionPerformed(ActionEvent e) 
		{
			Configuration conf = new Configuration();
			
			try{
				FileSystem hdfs=FileSystem.get(conf);
				 Path infile =new Path("hdfs:/hosp");
		    	    Path outfile =new Path("hdfs:/hosp1");
		 
		    	   // instream = new FileInputStream(infile);
		    	    FileStatus[]inF=hdfs.listStatus(infile);
		    	    FSDataOutputStream out = hdfs.create(outfile);
		    	    for(int i=0;i<inF.length;i++){
		    	    FSDataInputStream in=hdfs.open(inF[i].getPath());
		    	    byte[] buffer = new byte[256];
		 
		    	    int length=0;
		    	    while ((length = in.read(buffer)) > 0){
		    	    	out.write(buffer, 0, length);
		    	    }
		    	    textArea.append("Your Data Merged Successfully");
		    	    in.close();
		    	    
		    	    }
		    	    out.close();
		    	    

	   			}
	   			catch(Exception e1){}
		}
	   		 
	   		}
    class View3 implements ActionListener{

        public void actionPerformed(ActionEvent e) {
        	pig m=new pig();
        	ReadFile("hdfs:/mydata/part-r-00000");

        }}
    class View4 implements ActionListener{

        public void actionPerformed(ActionEvent e) {
        	Object[] options={"Eval","Import","Export"};
        	int n=JOptionPane.showOptionDialog(Frame,"Would you like to",null,JOptionPane.YES_NO_CANCEL_OPTION,JOptionPane.INFORMATION_MESSAGE,null, options,options[2]);
        	if(n==JOptionPane.YES_OPTION){
        		try {
					SqoopJavaInterface m=new SqoopJavaInterface();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
        	}
        	if(n==JOptionPane.NO_OPTION){
        		try {
					Sqoop m=new Sqoop();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
        	}
        	if(n==JOptionPane.CANCEL_OPTION){
        		try {
					sqoop1 m=new sqoop1();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
        	}
        }}
    
    class View5 implements ActionListener{

        public void actionPerformed(ActionEvent e) {
        	Object[] options={"Hadoop","Sqoop","Pig"};
        	int o=JOptionPane.showOptionDialog(Frame,"Would you like to",null,JOptionPane.YES_NO_CANCEL_OPTION,JOptionPane.INFORMATION_MESSAGE,null, options,options[2]);
        	if(o==JOptionPane.YES_OPTION){
        		try {
					hadoop m1=new hadoop();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
        	}
        	if(o==JOptionPane.NO_OPTION){
        		try {
					sqoopreset m1=new sqoopreset();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
        	}
        	if(o==JOptionPane.CANCEL_OPTION){
        		try {
					pigtest m1=new pigtest();
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
        	}
        }}
    class View6 implements ActionListener{

        public void actionPerformed(ActionEvent e) {
        	try {
				Spark b=new Spark();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        	
        }}
    
    class View2 implements ActionListener{
        public void actionPerformed(ActionEvent e) {
        	New n=new New(); 
        	ReadFile("hdfs:/hosp2/part-r-00000");
    	    
    }
    
    }}
class New{
	New(){
	Configuration conf = new Configuration();
	try{
	    Job job = Job.getInstance(conf, "Counting");
	    job.setJarByClass(New.class);
	    job.setMapperClass(M.class);
	    job.setReducerClass(R.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    Path pin= new Path("hdfs:/hosp1");
	    Path pout=new Path("hdfs:/hosp2");
	    FileInputFormat.addInputPath(job,pin);
	    FileOutputFormat.setOutputPath(job,pout);
	    job.waitForCompletion(true);
	  }
	catch(Exception e2)
	{e2.printStackTrace();}
}
public static class M extends Mapper<LongWritable, Text, Text, IntWritable>
{
	public void map(LongWritable key, Text compRecord, Context con)
			   throws IOException, InterruptedException {
			  String[] word = compRecord.toString().split(",");
			  String pati = word[2];
			  try {
			   int p_count =Integer.parseInt( word[3]);
			   con.write(new Text(pati), new IntWritable(p_count));
			   
			  } catch (Exception e) {
			   e.printStackTrace();
			  }
			 }
}
public static class R extends Reducer <Text,IntWritable,Text,IntWritable>
{
	public void reduce(Text key, Iterable<IntWritable> valueList,
			   Context con) throws IOException, InterruptedException {
			  try {
			   int count = 0;
			   for (IntWritable var : valueList) 
			   {
			   count++;
			   }
			    con.write(key, new IntWritable(count));
			   //output.collect(key, new IntWritable(sum));
			  } catch (Exception e) {
			   e.printStackTrace();
			  }
			 }
}

}
    