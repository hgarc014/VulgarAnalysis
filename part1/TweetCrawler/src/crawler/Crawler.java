package crawler;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.json.simple.JSONArray;

import crawler.TweetObjects.Tweet;
import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.UserMentionEntity;
import twitter4j.auth.AccessToken;
import twitter4j.json.DataObjectFactory;

public class Crawler /* extends Thread */ {
	CrawlerInformation info = null;
	long time;
	Boolean notfinished = true;

	private final static String DB_TwitterCrawled = "TwitterCrawled";
	private final static String TABLE_users = "users";
	private final static String TABLE_tweets = "tweets";
	private final static String TABLE_tweet_tags = "tweet_tags";
	private final static String TABLE_tweet_urls = "tweet_urls";

	Crawler(CrawlerInformation info) throws IOException {
		this.info = info;
	}

	public void saveTweets(List<Tweet> tweets) {

		if (tweets.size() > 0) {

			Statement st = null;
			ResultSet rs = null;

			
			String sql_users = null;
			String sql_tweets= null;
			String sql_tags= null;
			String sql_urls= null;
			try {

				HashMap<String, String> fields = tweets.get(0).getSQL();
				sql_users = "INSERT INTO " + DB_TwitterCrawled + "." + TABLE_users + " ("
						+ fields.get("user_fields") + ") Values ";
				sql_tweets = "INSERT INTO " + DB_TwitterCrawled + "." + TABLE_tweets + " ("
						+ fields.get("tweet_fields") + ") VALUES ";
				sql_tags = "INSERT INTO " + DB_TwitterCrawled + "." + TABLE_tweet_tags + " ("
						+ fields.get("tag_fields") + ") VALUES ";
				sql_urls = "INSERT INTO " + DB_TwitterCrawled + "." + TABLE_tweet_urls + " ("
						+ fields.get("url_fields") + ") VALUES ";
				for (Tweet tweet : tweets) {
					sql_users += "(" + tweet.getSQL().get("user_values") + "),";
					sql_tweets += "(" + tweet.getSQL().get("tweet_values") + "),";
					sql_tags += tweet.getSQL().get("tag_values");
					sql_urls += tweet.getSQL().get("url_values");
				}
				sql_users = sql_users.substring(0, sql_users.length() - 1);
				sql_users += " ON DUPLICATE KEY UPDATE user_id=user_id";

				sql_tweets = sql_tweets.substring(0, sql_tweets.length() - 1);
				sql_tweets += " ON DUPLICATE KEY UPDATE tweet_id=tweet_id";

				sql_tags = sql_tags.substring(0, sql_tags.length() - 1);

				sql_urls = sql_urls.substring(0, sql_urls.length() - 1);

				st = info.getDatabase().createStatement();
//				 System.out.println("USER:\n"+sql_users);
				st.executeUpdate(sql_users);
				st.close();

				st = info.getDatabase().createStatement();
//				 System.out.println("Tweets:\n"+sql_tweets);
				st.executeUpdate(sql_tweets);
				st.close();

				st = info.getDatabase().createStatement();
//				 System.out.println("tags:\n"+sql_tags);
				st.executeUpdate(sql_tags);
				st.close();

				st = info.getDatabase().createStatement();
//				 System.out.println("urls:\n"+sql_urls);
				st.executeUpdate(sql_urls);
				st.close();
				
				info.clearTweetsToSave();

				if (st != null) {
					st.close();
				}
				if (rs != null) {
					rs.close();
				}

			} catch (SQLException e) {
				
				System.out.println("\n\nERROR:" + e.getMessage());
				System.out.println("\n-----------------------------------\nUSER:\n"+sql_users);
				System.out.println("\n-----------------------------------\ntweets:\n"+sql_tweets);
				System.out.println("\n-----------------------------------\ntags:\n"+sql_tags);
				System.out.println("\n-----------------------------------\nurls:\n"+sql_urls);
				info.clearTweetsToSave();
//				System.exit(-1);
			}

		}

	}

	public void run() {
		// create twitterstream and fill in tokens
		final TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		AccessToken ac = new AccessToken(info.getAccToken(), info.getAccTokensec());
		twitterStream.setOAuthConsumer(info.getConsumer(), info.getConsumersec());
		twitterStream.setOAuthAccessToken(ac);

		FilterQuery filter = new FilterQuery();

		// languages and location to filter tweets
		String[] languages = { "en" };
		// location encompases most if not all of the entire world
		// double[][] locations = { { -180.0d, -90.0d }, { 180.0d, 90.0d } };

		// LOCATION OF USA = [-124.85, 24.39, -66.88, 49.38,]
		double[][] locations = { { -124.85d, 24.39d }, { -66.88d, 49.38d } };

		filter.language(languages);
		filter.locations(locations);

		final Timer t = new Timer();
		TimerTask stopListener = new TimerTask() {

			@Override
			public void run() {

				twitterStream.shutdown();
				twitterStream.cleanUp();
				if (!info.getTweetsToSave().isEmpty()) {
					int tweetsSaved = info.getTweetsToSave().size();
					saveTweets(info.getTweetsToSave());
					System.out.println("Saved " + tweetsSaved + " tweets to disk");
				}
				System.out.println("processed " + info.getTweetsObtained() + " in " + getTimeAgo(time));
			}
		};

		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {

				try {

					// TODO: save batch of tweets
					if (!info.getTweetsToSave().isEmpty() && (info.getTweetsToSave().size() >= info.getSaveEvery())) {
						int tweetsSaved = info.getTweetsToSave().size();
						saveTweets(info.getTweetsToSave());
						System.out.println("Saved " + tweetsSaved + " tweets to disk");
					}
					
					info.incrementTweetsObtained();
					Tweet tweet = new Tweet(status);
					info.getTweetsToSave().add(tweet);

//					if (status.getGeoLocation() != null) {
//						
//					} else {
//						// System.out.println(": Tweet has no location, moving
//						// to next tweet...");
//					}
				} catch (Exception e) {
					e.printStackTrace();
					// info.closeHashWriter();
					// info.closeTweetWriter();
				}
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		twitterStream.addListener(listener);
		// grab current time before starting connection
		time = System.currentTimeMillis();
		// t.scheduleAtFixedRate(stopListener, 60000 * 60, 60000 * 60);
		t.schedule(stopListener, 1000 * info.getRunForSeconds());

		twitterStream.filter(filter);
	}

	// used to return the size as a string
	public String getSize(long n) {
		int kb = (int) (n / 1000);
		int mb = kb / 1000;
		int gb = mb / 1000;
		if (gb != 0)
			return gb + "gbs";
		else if (mb != 0)
			return mb + "mbs";
		else
			return kb + "kbs";
	}

	// used for printing information of the tweet,
	// NOT used in crawling
	// public void printInformation(Status status) {
	// char[] charArray = new char[20];
	// Arrays.fill(charArray, '-');
	// String line = new String(charArray);
	// System.out.println(line + "\nUSER INFORMATION" + "\n@"
	// + status.getUser().getScreenName() + "\nTimeZone: "
	// + status.getUser().getTimeZone() + "\nUserLocation: "
	// + status.getUser().getLocation() + "\nFriends: "
	// + status.getUser().getFriendsCount() + "\nFollowers: "
	// + status.getUser().getFollowersCount()
	// + "\nProfileDescription: " + status.getUser().getDescription()
	// + "\nStatus: " + status.getUser().getStatus()
	// + "\nStatusCount: " + status.getUser().getStatusesCount()
	// + "\nLanguage: " + status.getUser().getLang());
	//
	// System.out.println("\nTWEET INFORMATION" + "\nTweetID: "
	// + status.getId() + "Language: " + status.getLang()
	// + "\nGeoLocation: " + status.getGeoLocation() + "\nRetweets: "
	// + status.getRetweetCount() + "\nFavorites: "
	// + status.getFavoriteCount() + "\nPlace: " + status.getPlace()
	// + "\nCreatedAt: " + status.getCreatedAt());
	//
	// System.out.println("\nMENTIONS:");
	// for (UserMentionEntity u : status.getUserMentionEntities()) {
	// System.out.println("@" + u.getScreenName());
	// }
	// System.out.println("\nPOUNDSIGNS:");
	// for (HashtagEntity h : status.getHashtagEntities()) {
	// System.out.println("#" + h.getText());
	// }
	// System.out.println("\nBody:\n" + status.getText());
	// }

	// return time took to crawl as string
	public String getTimeAgo(long t) {
		t = System.currentTimeMillis() - t;
		t /= 1000;
		if (t / 60 == 0)
			return t + " seconds";
		else if (t / 3600 == 0)
			return t / 60 + " minutes";
		else if (t / 86400 == 0)
			return t / 3600 + " hours";
		else
			return t / 86400 + " days";
	}

	// public void start() {
	// System.out.println("Starting " + threadName);
	// if (t == null) {
	// t = new Thread(this, threadName);
	// t.start();
	// }
	// }
}
