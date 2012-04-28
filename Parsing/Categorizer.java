package wikipediaMRAlgorithms.Parsing;

import java.util.HashMap;
import java.util.Map;

public class Categorizer {	
	private Map<String, String> _map;
	
	public Categorizer()
	{
		_map = new HashMap<String, String>();
		_map.put("news", "news");
		_map.put("newspaper", "news");
		_map.put("press", "news");
		_map.put("encyclopaedia", "encyclopaedia");
		_map.put("encyclopedia", "encyclopaedia");
		_map.put("web", "web");
		_map.put("newsgroup", "web");
		_map.put("website", "web");
		_map.put("book", "book");
		_map.put("conference", "conference");
		_map.put("album-notes", "album-notes");
		_map.put("article", "article");
		_map.put("comic", "comic");
		_map.put("document", "document");
		_map.put("episode", "episode");
		_map.put("interview", "interview");
		_map.put("journal", "journal");
		_map.put("magazine", "magazine");
		_map.put("map", "map");
		_map.put("movie", "movie");
		_map.put("quote", "quote");
		_map.put("report", "report");
		_map.put("speech", "speech");
		_map.put("video", "video");
		_map.put("wikisource", "wikisource");

	}
	public String getKey(String citationString){
		String key = _map.get(citationString);
		if(key == null)
			key = "unknown";
		
		return key;
	}
}
