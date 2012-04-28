package wikipediaMRAlgorithms.Tests;

import static org.hamcrest.CoreMatchers.any;
import static org.junit.Assert.*;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Vector;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import wikipediaMRAlgorithms.WebReferenceDomainCategorizer;
import wikipediaMRAlgorithms.Tests.Data.TestCites;

public class WebReferenceDomainCategorizerTests {

	private WebReferenceDomainCategorizer.MapClass _mapper;
	Context _output;
	public static final LongWritable one = new LongWritable(1);


	@Before
	public void setUp(){
		_mapper = new WebReferenceDomainCategorizer.MapClass();
		_output = mock(Context.class);

	}
		
	@Test
	public void EnsureWeCanCreateMapClassAndCallMapMethod() throws IOException, InterruptedException 
	{
		Vector<SimpleEntry<LongWritable, Text>> items  = TestCites.getItems();
		for(int i=0; i < items.size(); i++)
		{
			_mapper.map(items.elementAt(i).getKey(), items.elementAt(i).getValue(), _output);
		}
	}
	
	@Test
	public void EnsureWeCanParseTheCitationsAndAddOutput() throws IOException, InterruptedException
	{
		Vector<SimpleEntry<LongWritable, Text>> items  = TestCites.getItems();
		for(int i=0; i < items.size(); i++)
		{
			_mapper.map(items.elementAt(i).getKey(), items.elementAt(i).getValue(), _output);
		}
		verify(_output, times(593)).write(argThat(any(Text.class)), argThat(any(LongWritable.class)));
	}
	
	@Test
	public void EnsureWeCanParseTheCitationsAndAddSpecificURL() throws IOException, InterruptedException
	{
		Vector<SimpleEntry<LongWritable, Text>> items  = TestCites.getItems();
		for(int i=0; i < items.size(); i++)
		{
			_mapper.map(items.elementAt(i).getKey(), items.elementAt(i).getValue(), _output);
		}
		verify(_output, times(1)).write(eq(new Text("granitecityrollergirls.org")), argThat(any(LongWritable.class)));
	}
	
	@Test
	public void EnsureWeCanParseTheCitationsAndAddSpecificURLWithSubdomains() throws IOException, InterruptedException
	{
		Vector<SimpleEntry<LongWritable, Text>> items  = TestCites.getItems();
		for(int i=0; i < items.size(); i++)
		{
			_mapper.map(items.elementAt(i).getKey(), items.elementAt(i).getValue(), _output);
		}
		verify(_output, times(17)).write(eq(new Text("yahoo.com")), argThat(any(LongWritable.class)));
	}	
}
