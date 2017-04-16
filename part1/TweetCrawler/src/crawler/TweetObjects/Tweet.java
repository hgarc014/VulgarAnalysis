package crawler.TweetObjects;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.URLEntity;

//{

//urlEntities=
//[
//     URLEntityJSONImpl
//     {
//    	 url='https://t.co/YubGnFkO4r', 
//    	 expandedURL='http://bit.ly/2oa0kxb', 
//    	 displayURL='bit.ly/2oa0kxb'
//     }
//], 
//}

public class Tweet {

	private List<String> urls;

	private String text;
	private String source;
	private String inReplyToScreenName;
	private String lang;

	private Date createdAt;

	private Boolean isFavorited;
	private Boolean isRetweeted;
	private Boolean isTruncated;
	private Boolean isPossiblySensitive;

	private long[] contributors;

	private long id;
	private long inReplyToStatusId;
	private long inReplyToUserId;
	private long currentUserRetweetId;

	private int favoriteCount;
	private int retweetCount;

	private TweetUser user;
	private List<Hashtag> hashtags;
	private TweetPlace place;
	private GeoLocation location;
	private Tweet retweetedTweet;

	public Tweet(Status status) {

		urls = new ArrayList<>();
		if (status.getURLEntities() != null) {
			for (URLEntity u : status.getURLEntities()) {
				urls.add(u.getURL());
			}
		}

		this.text = status.getText();
		this.source = status.getSource();
		this.inReplyToScreenName = status.getInReplyToScreenName();
		this.lang = status.getLang();

		this.createdAt = status.getCreatedAt();

		this.isFavorited = status.isFavorited();
		this.isRetweeted = status.isRetweeted();
		this.isTruncated = status.isTruncated();
		this.isPossiblySensitive = status.isPossiblySensitive();

		this.contributors = status.getContributors();

		this.id = status.getId();
		this.inReplyToStatusId = status.getInReplyToStatusId();
		this.inReplyToUserId = status.getInReplyToUserId();
		this.currentUserRetweetId = status.getCurrentUserRetweetId();

		this.favoriteCount = status.getFavoriteCount();
		this.retweetCount = status.getRetweetCount();

		this.user = null;
		if (status.getUser() != null) {
			this.user = new TweetUser(status.getUser());
		}

		hashtags = new ArrayList<>();
		for (HashtagEntity ht : status.getHashtagEntities()) {
			hashtags.add(new Hashtag(ht.getText()));
		}

		this.place = null;
		if (status.getPlace() != null) {
			this.place = new TweetPlace(status.getPlace());
		}

		this.location = status.getGeoLocation();

		this.retweetedTweet = null;
		if (status.getRetweetedStatus() != null) {
			this.retweetedTweet = new Tweet(status.getRetweetedStatus());
		}

	}

	public void createTweetObject(ResultSet rs) {

	}

	public HashMap<String, String> getSQL() {

		HashMap<String, String> map = new HashMap<>();

		String[] tweet_fields = { "tweet_id", "tweet_text", "created_at", "geo_lat", "geo_long", "user_id",
				"screen_name", "name", "profile_image_url", "is_rt", "lang", "favorite_count", "retweet_count" };
		map.put("tweet_fields", getFieldStr(tweet_fields));

		Double lat = null;
		Double lng = null;
		if (location != null) {
			lat = location.getLatitude();
			lng = location.getLongitude();
		}

		String tweet_values = id + "," + getStringSQL(text) + "," + getStringSQL(createdAt) + "," + lat + "," + lng
				+ "," + user.getId() + "," + getStringSQL(user.getScreenName()) + "," + getStringSQL(user.getName())
				+ "," + getStringSQL(user.getProfileImageUrl()) + "," + isRetweeted + "," + getStringSQL(lang) + ","
				+ favoriteCount + "," + retweetCount;
		map.put("tweet_values", tweet_values);

		String[] user_fields = { "user_id", "screen_name", "name", "profile_image_url", "location", "url",
				"description", "created_at", "followers_count", "friends_count", "statuses_count", "time_zone" };
		map.put("user_fields", getFieldStr(user_fields));

		String user_values = user.getId() + "," + getStringSQL(user.getScreenName()) + ","
				+ getStringSQL(user.getName()) + "," + getStringSQL(user.getProfileImageUrl()) + ","
				+ getStringSQL(user.getLocation()) + "," + getStringSQL(user.getUrl()) + ","
				+ getStringSQL(user.getDescription()) + "," + getStringSQL(user.getCreatedAt()) + ","
				+ user.getFollowersCount() + "," + user.getFavoritesCount() + "," + user.getStatusesCount() + ","
				+ getStringSQL(user.getTimeZone());
		map.put("user_values", user_values);

		String[] tag_fields = { "tweet_id", "tag" };
		map.put("tag_fields", getFieldStr(tag_fields));
		String tag_values = "";
		for (Hashtag s : hashtags) {
			tag_values += "(" + id + "," + getStringSQL(s.getText()) + "),";
		}
		map.put("tag_values", tag_values);

		String[] url_fields = { "tweet_id", "url" };
		map.put("url_fields", getFieldStr(url_fields));
		String url_values = "";
		for (String s : urls) {
			url_values += "(" + id + "," + getStringSQL(s) + "),";
		}
		map.put("url_values", url_values);

		return map;
	}

	private String getFieldStr(String[] fields) {
		String ret = "";
		for (String s : fields) {
			ret += s + ",";
		}
		ret = ret.substring(0, ret.length() - 1);
		return ret;
	}

	private String getStringSQL(String field) {
		if (field == null) {
			return null;
		}
		String replacedStr = field.replace("\"", "\\\"");
		replacedStr = replacedStr.replace("'", "\\'");
		return "'" + replacedStr + "'";
	}

	private String getStringSQL(Date field) {
		java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "'" + sdf.format(field) + "'";
	}

}
