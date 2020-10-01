/**
 * 
 */
package br.ufc.great.hadoop.commons.utils;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import br.ufc.great.hadoop.commons.model.Tweet;

/**
 * @author Pedro Almir
 */
public class ReadTSV {
	
	/**
	 * @param line
	 * @return
	 * @throws IOException
	 */
	public static Tweet parse(String line) throws IOException {
		try (
			Reader reader = new StringReader(line);
			CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter('\t'));
		){
			CSVRecord csvRecord = csvParser.iterator().next();
			return new Tweet(csvRecord.get(0), csvRecord.get(1), csvRecord.get(7));
		}
	}
}
