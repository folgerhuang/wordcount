package org.sripe.sy;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WCReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

	
	public void reduce(Text key, Iterator<LongWritable> value, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter)
			throws IOException {
		int sum=0;
		while (value.hasNext()) {
			sum+=value.next().get();			
		}
		outputCollector.collect(key, new LongWritable(sum));
	}

}
