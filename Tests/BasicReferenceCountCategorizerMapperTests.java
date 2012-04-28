package wikipediaMRAlgorithms.Tests;

import static org.hamcrest.CoreMatchers.any;
import static org.junit.Assert.*;
import static org.junit.Before.*;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Vector;

import wikipediaMRAlgorithms.*;
import wikipediaMRAlgorithms.Tests.Data.TestList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

public class BasicReferenceCountCategorizerMapperTests {
	private BasicReferencCountCategorizerJob.MapClass _mapper;
	Context _output;
	
	
	@Before
	public void setUp(){
		_mapper = new BasicReferencCountCategorizerJob.MapClass();
		_output = mock(Context.class);
	}
	
	@Test
	public void EnsureWeCanCreateAMapper() {
		assertNotNull(new BasicReferencCountCategorizerJob.MapClass());
	}
	
	@Test
	public void EnsureTheMapperDoesntFailOnInput() throws IOException, InterruptedException{
		Vector<SimpleEntry<LongWritable, Text>> items = TestList.getItems();
		for(int i=0; i < items.size(); i++)
		{
			_mapper.map(items.elementAt(i).getKey(), items.elementAt(i).getValue(), _output);
		}
	}
	
	@Test
	public void EnsureTheMapperFindsAllTheNewArticles() throws IOException, InterruptedException{
		Vector<SimpleEntry<LongWritable, Text>> items = TestList.getItems();
		for(int i=0; i < items.size(); i++)
		{
			_mapper.map(items.elementAt(i).getKey(), items.elementAt(i).getValue(), _output);
		}
		verify(_output, times(3)).write(eq(new Text("news")), argThat(any(LongWritable.class)));

	}

}
