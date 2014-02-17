package com.machine.classify.group.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

import com.machine.classify.model.ClassesStats;

public class GrouppingDriver extends Configured implements Tool{
      public int run(String[] args) throws Exception
      {
            //creating a JobConf object and assigning a job name for identification purposes
            JobConf conf = new JobConf(getConf(), GrouppingDriver.class);
            conf.setJobName("Groupping");

            
            //Setting configuration object with the Data Type of output Key and Value
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            //Providing the mapper class for different languages
            conf.setMapperClass(EnglishMApper.class);
            conf.setMapperClass(FrenchMapper.class);
            conf.setMapperClass(GermanMapper.class);
            conf.setReducerClass(ClassAndAttributeMatrixReducer.class);

            
            //the hdfs input and output directory to be fetched from the command line
            MultipleInputs.addInputPath(conf, new Path("/usr/hduser/lan/en.txt"), TextInputFormat.class, EnglishMApper.class);
            MultipleInputs.addInputPath(conf, new Path("/usr/hduser/lan/fr.txt"), TextInputFormat.class, FrenchMapper.class);
            MultipleInputs.addInputPath(conf, new Path("/usr/hduser/lan/ge.txt"), TextInputFormat.class, GermanMapper.class);
            
            FileOutputFormat.setOutputPath(conf, new Path("/usr/hduser/output1/"));
            JobClient.runJob(conf);
            return 0;
      }
     
      public static void runhadoopForClassify() throws Exception
      {
    	  Configuration config=new Configuration();
    	  config.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
    	  config.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
           ToolRunner.run(config, new GrouppingDriver(),null);
      }
      public static void main(String[] args) throws Exception
      {	
    	  
    	  runhadoopForClassify();
    	 
    	  ClassesStats.intsertDataForClasses();
        
      }
}



