package crawler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json.simple.JSONObject;

import crawler.TweetObjects.Tweet;
import twitter4j.FilterQuery;
import twitter4j.Status;

public class CrawlerInformation {

	private List<Tweet> tweetsToSave = new ArrayList<Tweet>();

	private int numThreads = 1;
	private int saveEvery = 0;

	private long tweetsObtained = 0;
	private long tweetsSaved = 0;
	private int runForSeconds = 0;
	private FilterQuery filter = null;

	private String accToken = "2995123279-tPsou5RS11xE1I682qUtKiIYCRx4FeKCG4rXiGb";
	private String accTokensec = "pQfusO6QK6D8TwkN1MYorMjJWl2brx6fSj6CSfynK0Asw";
	private String consumer = "GQMH51Jbw075KnlYrcgSqPjhb";
	private String consumersec = "DCKu6VdwUgokz8drPmWqR6mFTBeDc6yyd7eoDMU23u0kUcYxm9";

	private static Connection database;

	private Boolean notFinished = true;

	CrawlerInformation(int numThreads, int saveEvery, int runForSeconds) {
		// this.fileSizes = fileSizes * convertToMB;
		// this.maxTweets = maxTweets;
		// this.outputdir = outputdir;
		this.numThreads = numThreads;
		this.runForSeconds = runForSeconds;
		// this.tweetWriter = tweetWriter;
		// this.hashWriter = hashWriter;
		// this.tweetFile = tweetFile;
		this.saveEvery = saveEvery;
	}

	public void setdbConnection(String host, String port, String user, String passwd) {
		try {
			database = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/", user, passwd);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void clearTweetsToSave(){
		this.tweetsToSave.clear();
	}

	public static Connection getDatabase() {
		return database;
	}

	public int getRunForSeconds() {
		return runForSeconds;
	}

	public int getSaveEvery() {
		return saveEvery;
	}

	public long getTweetsObtained() {
		return tweetsObtained;
	}

	public void incrementTweetsObtained() {
		++tweetsObtained;
	}

	public long getTweetsSaved() {
		return tweetsSaved;
	}

	public void incrementTweetsSaved() {
		++tweetsSaved;
	}

	public FilterQuery getFilter() {
		return filter;
	}

	public void setFilter(FilterQuery filter) {
		this.filter = filter;
	}

	public int getNumThreads() {
		return numThreads;
	}

	public Boolean getNotFinished() {
		return notFinished;
	}

	public List<Tweet> getTweetsToSave() {
		return tweetsToSave;
	}

	public void setNotFinished(Boolean notFinished) {
		this.notFinished = notFinished;
	}

	public String getAccToken() {
		return accToken;
	}

	public String getAccTokensec() {
		return accTokensec;
	}

	public String getConsumer() {
		return consumer;
	}

	public String getConsumersec() {
		return consumersec;
	}

}
