package org.sripe.sy;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.nfs.nfs3.response.WccAttr;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class WC extends Configured implements Tool {

	
	public int run(String[] args) throws Exception {
		JobConf conf=new JobConf();
		//conf.setJobName("WC");
		conf.setJarByClass(WccAttr.class);
		conf.addResource(new Path("/usr/local/hadoop-2.7.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.7.1/etc/hadoop/hdfs-site.xml"));
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		
		conf.setMapperClass(WCMapper.class);
		conf.setReducerClass(WCReducer.class);
		
		Path inPath=new Path(args[0]);
		Path outPath=new Path(args[1]);
		
		
		FileInputFormat.addInputPath(conf, inPath);
		FileOutputFormat.setOutputPath(conf, outPath);
		
		Job job = new Job(conf,"WordCount");	
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitcode=ToolRunner.run(new WC(), args);
		System.exit(exitcode);
	}

}
