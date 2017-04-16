package crawler.TweetObjects;

import java.util.ArrayList;

import twitter4j.GeoLocation;
import twitter4j.Place;

public class TweetPlace {
	private String name;
	private String streetAddress;
	private String countryCode;
	private String country;
	private String id;
	private String placeType;
	private String url;
	private String fullName;
	
	public TweetPlace(Place p) {
		this.name = p.getName();
		this.streetAddress = p.getStreetAddress();
		this.countryCode = p.getCountryCode();
		this.id = p.getId();
		this.placeType = p.getPlaceType();
		this.url = p.getURL();
		this.fullName = p.getFullName();
		this.country = p.getCountry();
	}

	public String getName() {
		return name;
	}

	public String getStreetAddress() {
		return streetAddress;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public String getCountry() {
		return country;
	}

	public String getId() {
		return id;
	}

	public String getPlaceType() {
		return placeType;
	}

	public String getUrl() {
		return url;
	}

	public String getFullName() {
		return fullName;
	}
	
}
