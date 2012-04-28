package wikipediaMRAlgorithms.Tests;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.hamcrest.CoreMatchers.any;
import static org.mockito.ArgumentMatcher.*;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;

import wikipediaMRAlgorithms.BasicReferencCountCategorizerJob;
import wikipediaMRAlgorithms.BasicReferenceExtractor;
import wikipediaMRAlgorithms.Tests.Data.TestArticle;

public class BasicReferenceExtratorTests {
	private BasicReferenceExtractor.MapClass _mapper;
	Context _output;
	
	
	@Before
	public void setUp(){
		_mapper = new BasicReferenceExtractor.MapClass();
		_output = mock(Context.class);
	}
	
	@Test
	public void EnsureWeCanCallTheMapMethod() throws IOException, InterruptedException {
		_mapper.map(null, TestArticle.getArticleText(), _output);		
	}
	
	@Test
	public void EnsureWhenAllCallsToTheMapperUseTestDataTitle() throws IOException, InterruptedException {
		_mapper.map(null, TestArticle.getArticleText(), _output);	
		
		verify(_output, times(25)).write(eq(new Text("Albedo")), argThat(any(Text.class)));
	}

}
