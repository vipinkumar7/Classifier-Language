package com.machine.classify.group.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ClassAndAttributeMatrixReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	// reduce method accepts the Key Value pairs from mappers, do the
	// aggregation based on keys and produce the final out put
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		

		Map<String,Integer> counter=new HashMap<String, Integer>();
		
		while (values.hasNext()) {
			String cached=values.next().toString();
			if (!counter.containsKey(cached))
			{
				
				counter.put(cached, 1);
			}
			
		}
		output.collect(key, new Text(String.valueOf(counter.size())));
	}
}
