package wikipediaMRAlgorithms.Tests.Data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.Text;

public class TestArticle {	
	private static final String FILENAME = "wikipediaMRAlgorithms/Tests/Data/article.xml";
	private static Text _articleText;
	private static String _articleString;
	
	public static Text getArticleText() throws IOException{
		if (_articleText == null)
			_articleText = new Text(readFileAsString(FILENAME));
		return _articleText;
	}
	
	public static String getArticleString() throws IOException{
		if(_articleString == null)
			_articleString = readFileAsString(FILENAME);
		return _articleString;
	}
	
	private static String readFileAsString(String filePath) throws java.io.IOException {
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(new InputStreamReader(TestArticle
            .class.getClassLoader().getResourceAsStream(filePath)));
        char[] buf = new char[1024];
        int numRead = 0;
        while ((numRead = reader.read(buf)) != -1) {
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
            buf = new char[1024];
        }
        reader.close();
        return fileData.toString();
    }
	
}
