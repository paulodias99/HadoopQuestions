/**
 * 
 */
package br.ufc.great.hadoop.commons.model;

import java.text.Normalizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

/**
 * @author Pedro Almir
 *
 */
public class Tweet {
	private String id;
	private String content;
	private Date createdAt;
	
	/**
	 * @param id
	 * @param content
	 * @param createdAt
	 */
	public Tweet(String id, String content, String createdAt) {
		try {
			this.id = id;
			this.content = content.trim();
			// ex.: Wed Oct 15 14:31:50 +0000 2014
			SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US);
			this.createdAt = formatter.parse(createdAt);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param id
	 * @param content
	 * @param createdAt
	 */
	public Tweet(String id, String content, Date createdAt) {
		this.id = id;
		this.content = content.trim();
		this.createdAt = createdAt;
	}
	
	/**
	 * @return n-gram
	 */
	public ArrayList<String> getNGrams() {
		ArrayList<String> result = new ArrayList<String>();
		result.addAll(this.getBigrams());
		result.addAll(this.getThreegrams());
		result.addAll(this.getFourgrams());
		return result;
	}
	
	/**
	 * @return bi-grams
	 */
	public ArrayList<String> getBigrams(){
		ArrayList<String> result = new ArrayList<String>();
		String[] parts = this.getCleanedContent().split(" ");
		for(int i = 0; i < parts.length - 1; i++) {
			result.add(parts[i] + " " + parts[i+1]);
		}
		return result;
	}
	
	/**
	 * @return three-grams
	 */
	public ArrayList<String> getThreegrams(){
		ArrayList<String> result = new ArrayList<String>();
		String[] parts = this.getCleanedContent().split(" ");
		for(int i = 0; i < parts.length - 2; i++) {
			result.add(parts[i] + " " + parts[i+1] + " " + parts[i+2]);
		}
		return result;
	}
	
	/**
	 * @return four-grams
	 */
	public ArrayList<String> getFourgrams(){
		ArrayList<String> result = new ArrayList<String>();
		String[] parts = this.getCleanedContent().split(" ");
		for(int i = 0; i < parts.length - 3; i++) {
			result.add(parts[i] + " " + parts[i+1] + " " + parts[i+2] + " " + parts[i+3]);
		}
		return result;
	}
	
	/**
	 * @return isRelatedDilma
	 */
	public boolean isRelatedDilma() {
		return this.content.toLowerCase().contains("dilma");
	}
	
	/**
	 * @return isRelatedAecio
	 */
	public boolean isRelatedAecio() {
		return this.content.toLowerCase().contains("aecio") || this.content.toLowerCase().contains("aécio");
	}
	
	/**
	 * @return
	 */
	private String getCleanedContent() {
		String cleaned = Normalizer.normalize(this.content, Normalizer.Form.NFD);
		return cleaned
				.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "")
				.replaceAll("http?://\\S+\\s?", "")
				.replaceAll("[^a-zA-Z0-9\\s]", " ")
				.replaceAll("\\b\\w{1,3}\\b\\s?", "")
				.replaceAll("\\s+", " ")
				.toLowerCase().trim();
	}
	
	/**
	 * @return
	 */
	public ArrayList<String> getHashTags(){
		ArrayList<String> result = new ArrayList<>();
		StringTokenizer itr = new StringTokenizer(this.content);
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			if(token.length() >= 2 && token.startsWith("#") && !token.subSequence(1, 2).equals("#")) {
				result.add(token);
			}
		}
		return result;
	}
	
	/**
	 * @return
	 */
	public int getHourOfDay() {
		Calendar c = Calendar.getInstance();
		c.setTime(this.createdAt);
		return c.get(Calendar.HOUR_OF_DAY);
	}
	
	/**
	 * @return
	 */
	public String getDayAndHour() {
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:00:00");
		return formatter.format(this.createdAt);
	}
	
	/**
	 * @return
	 */
	public String getFormattedDate() {
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM");
		return formatter.format(this.createdAt);
	}
	
	/**
	 * @return isMorning
	 */
	public boolean isMorning() {
		Calendar c = Calendar.getInstance();
		c.setTime(this.createdAt);
		int timeOfDay = c.get(Calendar.HOUR_OF_DAY);
		return (timeOfDay >= 0 && timeOfDay < 12) ? true : false;
	}
	
	/**
	 * @return isAfternoon
	 */
	public boolean isAfternoon() {
		Calendar c = Calendar.getInstance();
		c.setTime(this.createdAt);
		int timeOfDay = c.get(Calendar.HOUR_OF_DAY);
		return (timeOfDay >= 12 && timeOfDay < 18) ? true : false;
	}
	
	/**
	 * @return isAfternoon
	 */
	public boolean isNight() {
		Calendar c = Calendar.getInstance();
		c.setTime(this.createdAt);
		int timeOfDay = c.get(Calendar.HOUR_OF_DAY);
		return (timeOfDay >= 18 && timeOfDay < 24) ? true : false;
	}
	
	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}
	/**
	 * @return the content
	 */
	public String getContent() {
		return content;
	}
	/**
	 * @param content the content to set
	 */
	public void setContent(String content) {
		this.content = content;
	}
	/**
	 * @return the createdAt
	 */
	public Date getCreatedAt() {
		return createdAt;
	}
	/**
	 * @param createdAt the createdAt to set
	 */
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
	@Override
	public String toString() {
		return "Tweet [id=" + id + ", content=" + content + ", createdAt=" + createdAt + "]";
	}
}
