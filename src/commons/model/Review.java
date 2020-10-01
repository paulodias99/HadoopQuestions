/**
 * 
 */
package br.ufc.great.hadoop.commons.model;

import java.text.Normalizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

/**
 * @author Pedro Almir
 */
public class Review {
	private String id;
	private String title;
	private Date createdAt;
	private String text;
	
	/**
	 * @param id
	 * @param title
	 * @param createdAt
	 * @param text
	 */
	public Review(String id, String title, String createdAt, String text) {
		try {
			this.id = id;
			this.text = text;
			this.title = title;
			SimpleDateFormat formatter = new SimpleDateFormat("MMM dd, yyyy", Locale.US);
			this.createdAt = formatter.parse(createdAt);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param id
	 * @param title
	 * @param createdAt
	 * @param text
	 */
	public Review(String id, String title, Date createdAt, String text) {
		this.id = id;
		this.title = title;
		this.createdAt = createdAt;
		this.text = text;
	}
	
	/**
	 * @return
	 */
	public String getCleanedContent() {
		String cleaned = Normalizer.normalize(this.text, Normalizer.Form.NFD);
		return cleaned
				.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "")
				.replaceAll("http?://\\S+\\s?", "")
				.replaceAll("[^a-zA-Z0-9\\s]", " ")
				.replaceAll("[0-9\\s]", " ")
				.replaceAll("\\b\\w{1,3}\\b\\s?", "")
				.replaceAll("\\s+", " ")
				.toLowerCase().trim();
	}
	
	/**
	 * @return
	 */
	public String getFormattedDate() {
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		return formatter.format(this.createdAt);
	}
	
	/**
	 * @return n-gram
	 */
	public ArrayList<String> getNGrams() {
		ArrayList<String> result = new ArrayList<String>();
		result.addAll(this.getBigrams());
		result.addAll(this.getThreegrams());
		result.addAll(this.getFourgrams());
		result.addAll(this.getFivegrams());
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
	 * @return four-grams
	 */
	public ArrayList<String> getFivegrams(){
		ArrayList<String> result = new ArrayList<String>();
		String[] parts = this.getCleanedContent().split(" ");
		for(int i = 0; i < parts.length - 4; i++) {
			result.add(parts[i] + " " + parts[i+1] + " " + parts[i+2] + " " + parts[i+3] + " " + parts[i+4]);
		}
		return result;
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
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}
	/**
	 * @param title the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
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
	/**
	 * @return the text
	 */
	public String getText() {
		return text;
	}
	/**
	 * @param text the text to set
	 */
	public void setText(String text) {
		this.text = text;
	}
	@Override
	public String toString() {
		return "Review [id=" + id + ", title=" + title + ", createdAt=" + createdAt + ", text=" + text + "]";
	}
}
