package com.machine.classify.learn.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.machine.classify.model.AttributeClassData;

public class ConditionalProbabilityDriver {
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		/*fill the attribute data first*/
		AttributeClassData.getAttributeData();
		
		
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
	     DBConfiguration.configureDB(conf,
	     "com.mysql.jdbc.Driver",   // driver class
	     "jdbc:mysql://localhost:3306/classify", // db url
	     "root",    // user name
	     "root"); //password

	     Job job = new Job(conf);
	     job.setJarByClass(ConditionalProbabilityDriver.class);
	     job.setMapperClass(ConditionalProbabilityMapper.class);
	     job.setReducerClass(ConditionalProbabilityReducer.class);
	     job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(Text.class);
	     job.setOutputKeyClass(DBOutputWritable.class);
	     job.setOutputValueClass(NullWritable.class);
	     job.setInputFormatClass(TextInputFormat.class);
	     job.setOutputFormatClass(DBOutputFormat.class);

	     FileInputFormat.addInputPath(job, new Path("/usr/hduser/output3/part-00000"));
	     DBOutputFormat.setOutput(job, "attribute_probability", new String[]{"attribute","english","french","german"});
	     System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
