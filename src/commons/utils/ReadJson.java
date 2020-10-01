/**
 * 
 */
package br.ufc.great.hadoop.commons.utils;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import br.ufc.great.hadoop.commons.model.Review;

/**
 * @author Pedro Almir
 *
 */
public class ReadJson {
	
	@SuppressWarnings("rawtypes")
	public static List<Review> getReviews(String json){
		ArrayList<Review> result = new ArrayList<>();
		JsonReader reader = new JsonReader(new StringReader(json));
		reader.setLenient(true);
		
		List<Map> jsonMap = new Gson().fromJson(reader, new TypeToken<List<Map>>() {}.getType());
		for(Map obj : jsonMap) {
			String id = (String) ((Map) obj.get("_id")).get("$oid");
			String title = (String) (obj.get("title"));
			String createdAt = (String) (obj.get("createdAt"));
			String text = (String) (obj.get("text"));
			
			result.add(new Review(id, title, createdAt, text));
		}
		
		return result;
	}
	
	@SuppressWarnings("rawtypes")
	public static Review getReview(String json){
		JsonReader reader = new JsonReader(new StringReader(json));
		reader.setLenient(true);
		
		Map jsonMap = new Gson().fromJson(reader, Map.class);
		String id = (String) ((Map) jsonMap.get("_id")).get("$oid");
		String title = (String) (jsonMap.get("title"));
		String createdAt = (String) (jsonMap.get("createdAt"));
		String text = (String) (jsonMap.get("text"));
		
		return new Review(id, title, createdAt, text);
	}
}
