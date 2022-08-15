package mapred;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;



/*
Imported External JAR Files

hadoop-3.2.2\share\hadoop\common\lib\commons-logging-1.1.3.jar
hadoop-3.2.2\share\hadoop\common\lib\guava-27.0-jre.jar
hadoop-3.2.2\share\hadoop\common\hadoop-common-3.2.2.jar
hadoop-3.2.2\share\hadoop\mapreduce\hadoop-mapreduce-client-app-3.2.2.jar
hadoop-3.2.2\share\hadoop\mapreduce\hadoop-mapreduce-client-common-3.2.2.jar
hadoop-3.2.2\share\hadoop\mapreduce\hadoop-mapreduce-client-core-3.2.2.jar
hadoop-3.2.2\share\hadoop\mapreduce\hadoop-mapreduce-client-hs-3.2.2.jar
hadoop-3.2.2\share\hadoop\mapreduce\hadoop-mapreduce-client-hs-plugins-3.2.2.jar
hadoop-3.2.2\share\hadoop\mapreduce\hadoop-mapreduce-client-jobclient-3.2.2.jar
hadoop-3.2.2\share\hadoop\mapreduce\hadoop-mapreduce-client-jobclient-3.2.2-tests.jar
hadoop-3.2.2\share\hadoop\mapreduce\hadoop-mapreduce-client-nativetask-3.2.2.jar
hadoop-3.2.2\share\hadoop\mapreduce\hadoop-mapreduce-client-shuffle-3.2.2.jar
hadoop-3.2.2\share\hadoop\mapreduce\hadoop-mapreduce-client-uploader-3.2.2.jar

*/
public class MapRed {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> { 		 
		//IntWritable EmpNo = new IntWritable();
		Text EmpNo = new Text();
		Text CertificationCode_EmpNo = new Text();
		Text CertificationCode = new Text();
		
		//Text EmpPUCode = new Text();
		//Text CertificationTitle = new Text();
		//Text Result = new Text();
		//Text RoleCapability = new Text();
		
        public void map(LongWritable key, Text value,
        				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        	
    		/*  PARAMETERS EXPLANATION
    		 *  LongWritable, IntWritable: Hadoop serializable type for holding numericals
    		 *  Text: Hadoop serializable type for holding String
    		 *  OutputCollector: Hadoop type for holding the mapper output temporarily in local data node's disk
    		 */
        	
          String line = value.toString();
          String fields[] = line.split(",");
          
          //EXTRACT FIELDS
          //EmpNo.set(Integer.parseInt(fields[Constants.EmpNo]));
          EmpNo.set(fields[Constants.EmpNo]);
          CertificationCode_EmpNo.set(fields[Constants.CertificationCode]+fields[Constants.EmpNo]);
          CertificationCode.set(fields[Constants.CertificationCode]);
          //RoleCapability.set(fields[Constants.RoleCapability]);
          //EmpPUCode.set(fields[Constants.EmpPUCode]);          
          //CertificationTitle.set(fields[Constants.CertificationTitle]);
          //Result.set(fields[Constants.Result]);
          
        	
          //FIND TOTAL NUMBER OF ATTEMPTS FOR A SPECIFIC CERTIFICATION (I)
          output.collect(CertificationCode, new IntWritable(1));
          //FIND TOTAL NUMBER OF EMPLOYEES QUALIFIED FOR EACH CERTIFICATION (II)
          output.collect(CertificationCode_EmpNo, new IntWritable(1));
          //FIND TOTAL NUMBER OF ONSITE EMPLOYEES WHO HAVE ATTEMPTED THE CERTIFICATION (III)
          if (fields[Constants.OnSiteOffShore] == Constants.ONSITE) {
        	  output.collect(CertificationCode, new IntWritable(1));
          }

        }
    }
	
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	    public void reduce(Text key, Iterator<IntWritable> values,
	    				   OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	      
	      /*
	       * Text : Hadoop serializable type for holding key String
	       * Iterator : For holding the array of numerical type
	       * OutputCollector : Hadoop type for holding the intermediate output temporarily in local data node's disk
	       * 
	       */
	      int sum = 0;
	      while (values.hasNext()) {
	        sum += values.next().get();
	      }
	      output.collect(key, new IntWritable(sum));
	    }
	  }
	
	public static void main(String[] args) throws Exception {
		    JobConf conf = new JobConf(MapRed.class);
		    conf.setJobName("capstone_mapred");

		    conf.setOutputKeyClass(Text.class);
		    conf.setOutputValueClass(IntWritable.class);

		    conf.setMapperClass(Map.class);
		    conf.setCombinerClass(Reduce.class);
		    conf.setReducerClass(Reduce.class);

		    conf.setInputFormat(TextInputFormat.class);
		    conf.setOutputFormat(TextOutputFormat.class);

		    FileInputFormat.setInputPaths(conf, new Path(args[0]));
		    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		    JobClient.runJob(conf);
	}

	
}
