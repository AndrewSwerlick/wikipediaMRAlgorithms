package wikipediaMRAlgorithms;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import wikipediaMRAlgorithms.Parsing.Categorizer;

public class BasicReferencCountCategorizerJob {	
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		private Categorizer _categorizer;
		public MapClass(){
			_categorizer = new Categorizer();
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] line = value.toString().split("\\t");
			Text newKey = new Text(line[0]);
			LongWritable newValue = new LongWritable(Long.parseLong(line[1]));
			String citationCategory = _categorizer.getKey(newKey.toString());
			context.write(new Text(citationCategory), newValue);
		}
	}
	
	public static void main(String args[]) throws Exception{
		Configuration categorizerConf = new Configuration();
        Job job2 = new Job(categorizerConf, "Wikipedia citation grouped count");
        job2.setJarByClass(BasicReferencCountCategorizerJob.class);
        job2.setMapperClass(BasicReferencCountCategorizerJob.MapClass.class);
        job2.setReducerClass(LongSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
    	FileInputFormat.setInputPaths(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        
        	
        System.exit(job2.waitForCompletion(true) ? 0:1);
	}
}
