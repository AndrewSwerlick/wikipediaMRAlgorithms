package wikipediaMRAlgorithms;

import java.io.IOException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.mahout.classifier.bayes.XmlInputFormat;

public class BasicReferenceExtractor {
	public static class MapClass extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Pattern citationPattern = Pattern.compile("\\{\\{(?i)cite.*?\\}\\}", Pattern.DOTALL | Pattern.MULTILINE);
			Pattern titlePattern = Pattern.compile("<title>.*?</title>", Pattern.DOTALL);
			
			Matcher citationMatcher = citationPattern.matcher(value.toString());
			Matcher titleMatcher = titlePattern.matcher(value.toString());
			
			String title = "";
			if(titleMatcher.find())
				title = titleMatcher.group().replace("<title>", "").replace("</title>", "");

			while(citationMatcher.find()){
			   String matchedLine = citationMatcher.group().replace("{","").replace("}","").replace("\n", " ").replace("\r", " ");			   
			   context.write(new Text(title), new Text(matchedLine));
		   }
			
		}
		
	}
	
	public static void runJob(Path input, Path output) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		Job job = new Job(conf, "Wikipedia cititation type count");		
		
		job.setJarByClass(BasicReferenceExtractor.class);
		job.setMapperClass(BasicReferenceExtractor.MapClass.class);
		job.setReducerClass(Reducer.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(XmlInputFormat.class);
		
		FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.waitForCompletion(true);
	}
		
	public static void main(String args[]) throws Exception{
		runJob(new Path(args[0]), new Path(args[1]));
	}
}
