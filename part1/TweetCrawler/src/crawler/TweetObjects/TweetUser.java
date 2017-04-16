package crawler.TweetObjects;

import java.util.Date;

import twitter4j.User;

public class TweetUser {

	private long id;
	private String name;
	private String screenName;
	private String location;
	private String description;
	private Boolean isContributorsEnabled;
	private String profileImageUrl;
	private String profileImageUrlHttps;
	private Boolean isDefaultProfileImage;
	private String url;
	private Boolean isProtected;
	private int followersCount;
	private Date createdAt;
	private int favoritesCount;
	private String timeZone;
	private String lang;
	private int statusesCount;
	private Boolean isGeoEnabled;
	private Boolean isVerified;
	private Boolean isTranslator;
	private int listedCount;
	private Boolean isFollowRequestSent;

	public TweetUser(User u) {
		this.id = u.getId();
		this.name = u.getName();
		this.screenName = u.getScreenName();
		this.location = u.getLocation();
		this.description = u.getDescription();
		this.isContributorsEnabled = u.isContributorsEnabled();
		this.profileImageUrl = u.getProfileImageURL();
		this.profileImageUrlHttps = u.getProfileImageURLHttps();
		this.isDefaultProfileImage = u.isDefaultProfileImage();
		this.url = u.getURL();
		this.isProtected = u.isProtected();
		this.followersCount = u.getFollowersCount();
		this.createdAt = u.getCreatedAt();
		this.favoritesCount = u.getFavouritesCount();
		this.timeZone = u.getTimeZone();
		this.lang = u.getLang();
		this.statusesCount = u.getStatusesCount();
		this.isGeoEnabled = u.isGeoEnabled();
		this.isVerified = u.isVerified();
		this.isTranslator = u.isTranslator();
		this.listedCount = u.getListedCount();
		this.isFollowRequestSent = u.isFollowRequestSent();
	}

	public long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getScreenName() {
		return screenName;
	}

	public String getLocation() {
		return location;
	}

	public String getDescription() {
		return description;
	}

	public Boolean getIsContributorsEnabled() {
		return isContributorsEnabled;
	}

	public String getProfileImageUrl() {
		return profileImageUrl;
	}

	public String getProfileImageUrlHttps() {
		return profileImageUrlHttps;
	}

	public Boolean getIsDefaultProfileImage() {
		return isDefaultProfileImage;
	}

	public String getUrl() {
		return url;
	}

	public Boolean getIsProtected() {
		return isProtected;
	}

	public int getFollowersCount() {
		return followersCount;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public int getFavoritesCount() {
		return favoritesCount;
	}

	public String getTimeZone() {
		return timeZone;
	}

	public String getLang() {
		return lang;
	}

	public int getStatusesCount() {
		return statusesCount;
	}

	public Boolean getIsGeoEnabled() {
		return isGeoEnabled;
	}

	public Boolean getIsVerified() {
		return isVerified;
	}

	public Boolean getIsTranslator() {
		return isTranslator;
	}

	public int getListedCount() {
		return listedCount;
	}

	public Boolean getIsFollowRequestSent() {
		return isFollowRequestSent;
	}
	
}
