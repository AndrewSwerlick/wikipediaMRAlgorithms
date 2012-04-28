package wikipediaMRAlgorithms;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.mahout.classifier.bayes.*;

import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


public class BasicReferenceCountJob{
	
	public static class MapClass extends Mapper<Object, Text, Text, LongWritable>{
		public static final LongWritable one = new LongWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Pattern p = Pattern.compile("\\{\\{(?i)cite.*?\\}\\}", Pattern.DOTALL | Pattern.MULTILINE);
			Matcher m = p.matcher(value.toString());
			while(m.find()){
			   String matchedLine = m.group().replace("{","").replace("}","");
			   String[] splitCitation = matchedLine.split("[\\s\\|]");
			   if(splitCitation.length>=2)
			   {
				   splitCitation[0].replaceAll("[&/r/n:};!~]+$", "");
				   String type = "";
				   	if(splitCitation.length>=1 && splitCitation[0].trim().compareToIgnoreCase("cite") != 0)
				   		type = splitCitation[0].replace("cite", "");
				   	else
				   		type = splitCitation[1].trim();
				   	context.write(new Text(type), one);
			   }
			   else
				   context.write(new Text("unknown"),one);
				   
		   }
		}	
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		Job job = new Job(conf, "Wikipedia cititation type count");		
		
		job.setJarByClass(BasicReferenceCountJob.class);
		job.setMapperClass(BasicReferenceCountJob.MapClass.class);
		job.setReducerClass(LongSumReducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(XmlInputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);                	
	}
}
