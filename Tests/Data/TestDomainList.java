package wikipediaMRAlgorithms.Tests.Data;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Vector;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TestDomainList extends TestList{
	protected static String getFILENAME()
	{ 
		return "wikipediaMRAlgorithms/Tests/Data/domainList.txt";
	}
	
	public static Vector<SimpleEntry<LongWritable, Text>> getItems() throws IOException{
		return readItemsFromFile();					
	}
	
	private static Vector<SimpleEntry<LongWritable, Text>> readItemsFromFile() throws IOException	 
	{
		Vector<SimpleEntry<LongWritable, Text>> list = new Vector<SimpleEntry<LongWritable, Text>>();

		
		  // Open the file that is the first 
		  // command line parameter
		  InputStream fstream = TestList.class.getClassLoader().getResourceAsStream(getFILENAME());
		  // Get the object of DataInputStream
		  DataInputStream in = new DataInputStream(fstream);
		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
		  String strLine;
		  //Read File Line By Line
		  while ((strLine = br.readLine()) != null)   {
			  list.add(new SimpleEntry<LongWritable, Text>(new LongWritable(1), new Text(strLine)));
		  }
		  
		  //Close the input stream
		  in.close();
			
		return list;
	}
}
