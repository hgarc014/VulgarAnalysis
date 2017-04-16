package crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;



public class Main extends Thread {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		int saveEvery = 100;
		int threads = 1;
		int runForSeconds = 10;

		if(args.length == 1){
			runForSeconds = checkNumber(args[0], "Number of Seconds");

			CrawlerInformation info = new CrawlerInformation(threads, saveEvery,runForSeconds);
			info.setdbConnection("localhost", "3306", "henry", "PH.h3143,twitter");

			Crawler c = new Crawler(info);
			c.run();

		}else{
			 System.out.println("Invalid number of arguments");
			 System.out.println("Arguments: " + args.length);
			 System.out.println("<Number of Seconds>");			
		}
		
	}
	
	public static Integer checkNumber(String convert, String name) {
		try {
			int x = Integer.valueOf(convert);
			if (x == 0) {
				System.out.println("WHOA! You passed " + x + " for " + name + ". We will convert it to 1");
				x = 1;
			} else if (x < 0) {
				int y = -1 * x;
				System.out.println("WHOA! You passed " + x + " for " + name + ". We will convert it to " + y);
				x = y;
			}
			return x;
		} catch (NumberFormatException n) {
			System.out.println(convert + " is not a valid integer value for " + name + "!");
			System.exit(0);
			return 0;
		}
	}
}
