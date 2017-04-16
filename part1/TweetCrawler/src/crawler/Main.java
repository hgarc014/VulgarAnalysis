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

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;


public class Main extends Thread {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		int saveEvery = 100;
		int threads = 1;
		int runForSeconds = 10;

//		if(args.length == 1){
//			runForSeconds = checkNumber(args[0], "Number of Seconds");
//
//			CrawlerInformation info = new CrawlerInformation(threads, saveEvery,runForSeconds);
//			info.setdbConnection("localhost", "3306", "henry", "PH.h3143,twitter");
//
//			Crawler c = new Crawler(info);
//			c.run();
//
//		}else{
//			 System.out.println("Invalid number of arguments");
//			 System.out.println("Arguments: " + args.length);
//			 System.out.println("<Number of Seconds>");			
//		}
		
		CrawlerInformation info = new CrawlerInformation(threads, 100, 10);
		info.setdbConnection("localhost", "3306", "henry", "PH.h3143,twitter");
		
		Statement st;
		ResultSet rs ;
		try {
			st = info.getDatabase().createStatement();
			rs = st.executeQuery("SELECT * FROM TwitterCrawled.tweets");
			
			Set<Integer> s = new HashSet<>();
			String tweet  = null;
//			String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
	        String regex = "(http?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]"; // does not match <http://google.com>
	        String regex_https = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]"; // does not match <http://google.com>
	        
	        Integer res;
			while(rs.next()){
				tweet = rs.getString("tweet_text");
				tweet = tweet.replaceAll(regex, "");
				tweet = tweet.replaceAll(regex_https, "");
				res = findSentiment(tweet);
				if(!s.contains(res)){
					s.add(res);
				}
//				System.out.println(findSentiment(tweet) + ":" + tweet );
			}
			
			for(Integer i :s){
				System.out.println(i);
			}
//			System.out.println("BAD:"+findSentiment("Fuck you, asshole."));
//			System.out.println("Good:"+findSentiment("Whoever you are, you are really chill and awesome."));
//			System.out.println("Good:"+findSentiment("Love is the best thinig in the world."));
//			System.out.println("Good:"+findSentiment("I love you."));
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static StanfordCoreNLP pipeline = null;
	public static int findSentiment(String tweet){
		if(pipeline == null){
			pipeline = new StanfordCoreNLP("MyPropFile.properties");
		}
		int mainSentiment = 0;
		if (tweet != null && tweet.length() > 0) {
			int longest = 0;
			Annotation annotation = pipeline.process(tweet);
			for (CoreMap sentence : annotation
					.get(CoreAnnotations.SentencesAnnotation.class)) {
				Tree tree = sentence
						.get(SentimentAnnotatedTree.class);
				int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
				String partText = sentence.toString();
				if (partText.length() > longest) {
					mainSentiment = sentiment;
					longest = partText.length();
				}

			}
		}
		return mainSentiment;	
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
