package wikipediaMRAlgorithms.Tests;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.hamcrest.CoreMatchers.any;
import static org.mockito.ArgumentMatcher.*;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.matchers.Any;

import wikipediaMRAlgorithms.*;
import wikipediaMRAlgorithms.Tests.Data.TestArticle;

public class BasicReferenceCountMapperTests {

	BasicReferenceCountJob.MapClass _mapper;
	Context _output;
	Reporter _reporter;

	@Before
	public void setup()
	{
		_mapper = new BasicReferenceCountJob.MapClass();
		_output = mock(Context.class);
	}
	
	@Test
	public void ensureWeCanCreateAMapper() {
		assertNotNull(new BasicReferenceCountJob.MapClass());
	}

	
	@Test
	public void ensureWeCanRunTheMapMethodWithTestInput() throws IOException, InterruptedException{
		_mapper.map(null, TestArticle.getArticleText(), _output);		
	}
	
	@Test 
	public void ensureWeCollectAtLeastOneOutputValue() throws IOException, InterruptedException{
		_mapper.map(null, TestArticle.getArticleText(), _output);
		verify(_output, atLeastOnce()).write(argThat(any(Text.class)), eq(new LongWritable(1)));
	}	
	
	@Test
	public void ensureWeCollect_25_OutputValues() throws IOException, InterruptedException{
		_mapper.map(null, TestArticle.getArticleText(), _output);
		verify(_output, times(25)).write(argThat(any(Text.class)), eq(new LongWritable(1)));
	}
	
	@Test
	public void ensureWeCollect_6_OutputValuesForJournals() throws IOException, InterruptedException{
		_mapper.map(null, TestArticle.getArticleText(), _output);
		verify(_output, times(6)).write(eq(new Text("journal")), eq(new LongWritable(1)));	
	}
	
	@Test
	public void ensureWeCollect_17_OutputValuesForWebs() throws IOException, InterruptedException{
		_mapper.map(null, TestArticle.getArticleText(), _output);
		verify(_output, times(17)).write(eq(new Text("web")), eq(new LongWritable(1)));	
	}
	
}
