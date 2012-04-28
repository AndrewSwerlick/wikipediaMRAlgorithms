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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.mahout.classifier.bayes.XmlInputFormat;

public class WebReferenceDomainCategorizer {
	public static final LongWritable one = new LongWritable(1);

	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable>{
		public void map(LongWritable key, Text value, Context output) throws InterruptedException, IOException
		{
			Pattern urlPattern = Pattern.compile("[\\s\\|\\{]url=http://.*?[\\|\\}]", Pattern.DOTALL | Pattern.MULTILINE);
			Matcher urlMatcher = urlPattern.matcher(value.toString());
			
			String fullDomain = "";
			if(urlMatcher.find())
			{
				fullDomain = urlMatcher.group().replace("url=http://", "");
				int backslashInd = fullDomain.indexOf('/');
				if(backslashInd != -1) 				
					fullDomain = fullDomain.substring(0, backslashInd).replace("/", "").replace("|", "").trim();				
				else				
					fullDomain = fullDomain.replace("/", "").replace("|", "").trim();
				
				String[] domainParts = fullDomain.split("\\.");
				if(domainParts.length > 2 && domainParts[domainParts.length-1] != "uk")
				{
					String rootDomain = domainParts[domainParts.length - 2] + "." + domainParts[domainParts.length -1];
					output.write(new Text(rootDomain), one);	
				}
				else if(domainParts.length > 3){
					String rootDomain = domainParts[domainParts.length - 3] + "." 
									  + domainParts[domainParts.length -2] + "." 
									  + domainParts[domainParts.length -1];
					output.write(new Text(rootDomain), one);	
				}				
			}
		}
			
	}
	public static void runJob(Path input, Path output) throws IOException, InterruptedException, ClassNotFoundException{		
                
		Configuration categorizerConf = new Configuration();
        Job job2 = new Job(categorizerConf, "web reference count");
        job2.setJarByClass(WebReferenceDomainCategorizer.class);
        job2.setMapperClass(WebReferenceDomainCategorizer.MapClass.class);
        job2.setReducerClass(LongSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
    	FileInputFormat.setInputPaths(job2, input);
        FileOutputFormat.setOutputPath(job2, output);
                	
        System.exit(job2.waitForCompletion(true) ? 0:1);
	}
	
	public static void main(String[] args) throws Exception{		
		runJob(new Path(args[0]), new Path(args[1]));
	}	
	
}
