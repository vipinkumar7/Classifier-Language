
/**
 * No copyright its free to use change and distribute
 */
package com.machine.classify.group.hadoop;
import java.io.IOException;
import java.util.Locale;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.machine.classify.model.ClassesStats;

/**
 * 
 * @author Vipin Kumar
 * 
 */
public class EnglishMApper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	
	private Text word = new Text();
	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String line = value.toString();
		/*remove all the punctuation and extra spaces*/
		line=line.replaceAll("\\p{P}", " ").replaceAll("\\s+", " ").toLowerCase(Locale.getDefault());
		
		
		/*lets consider all the lines as one document and proceed*/
		
		ClassesStats.english.incrementAndGet();
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken()+"\t"+"English");
			output.collect(word, new Text(key.toString()));
		}
		
	}
}
