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

import wikipediaMRAlgorithms.TopLevelReferenceDomainCategorizer;
import wikipediaMRAlgorithms.WebReferenceDomainCategorizer;
import wikipediaMRAlgorithms.Tests.Data.TestCites;

public class TopLevelReferenceDomainCategorizerTests {

	private TopLevelReferenceDomainCategorizer.MapClass _mapper;
	Context _output;
	public static final LongWritable one = new LongWritable(1);
	
	@Before
	public void setUp(){
		_mapper = new TopLevelReferenceDomainCategorizer.MapClass();
		_output = mock(Context.class);

	}
	
	@Test
	public void EnsureWeCanConstructATopLevelReferenceDomainCategorizer() {
		new TopLevelReferenceDomainCategorizer();
	}
	
	@Test
	public void EnsureWeCount352DotComs() throws InterruptedException, IOException{
		Vector<SimpleEntry<LongWritable, Text>> items  = TestCites.getItems();
		for(int i=0; i < items.size(); i++)
		{
			_mapper.map(items.elementAt(i).getKey(), items.elementAt(i).getValue(), _output);
		}
		verify(_output, times(352)).write(eq(new Text("com")), eq(one));
	}

}
