/**
 * 
 */
package br.ufc.great.hadoop.commons.utils;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Pedro Almir
 *
 */
public class MyFileUtils {
	public static void main(String[] args) {
		StringTokenizer tokenizer = new StringTokenizer(" Pedro Almir Pedro Almir");
		while (tokenizer.hasMoreTokens()) {
			System.out.println(tokenizer.nextToken().trim());
		}
	}
	public static void delete(File file) throws IOException {
		if (file.isDirectory()) {
			// directory is empty, then delete it
			if (file.list().length == 0) {
				file.delete();
				System.out.println("Directory is deleted : " + file.getAbsolutePath());
			} else {
				// list all the directory contents
				String files[] = file.list();

				for (String temp : files) {
					// construct the file structure
					File fileDelete = new File(file, temp);
					// recursive delete
					delete(fileDelete);
				}

				// check the directory again, if empty then delete it
				if (file.list().length == 0) {
					file.delete();
					System.out.println("Directory is deleted : " + file.getAbsolutePath());
				}
			}

		} else {
			// if file, then delete it
			file.delete();
			System.out.println("File is deleted : " + file.getAbsolutePath());
		}
	}
}
