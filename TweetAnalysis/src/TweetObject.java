import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.json.simple.JSONObject;

public class TweetObject implements Writable {

	// private double score;
	private int score;
	private String id;
	private String lat;
	private String lon;
	private String text;
	// private Sotring fullName;
	private String username;
	private int total = 0;
	private int badTotal = 0;
	private double realScore = 0;
	private double badScore = 0;

	public TweetObject() {
		super();
	}

	public TweetObject(int score, String id, String lat, String lon, String text, String username) {
		super();
		this.score = score;
		this.id = id;
		this.lat = lat;
		this.lon = lon;
		this.text = text;
		this.username = username;
	}

	public void updateTotal(int val) {
		total += val;
	}

	public void updateBadTotal(int val) {
		badTotal += val;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	public void setBadTotal(int total) {
		this.badTotal = total;
	}

	public int getBadTotal() {
		return badTotal;
	}

	public int getTotal() {
		return total;
	}

	public int getScore() {
		return score;
	}

	public String getId() {
		return id;
	}

	public String getLat() {
		return lat;
	}

	public String getLon() {
		return lon;
	}

	public String getText() {
		return text;
	}

	public String getUsername() {
		return username;
	}

	public void setScore(int score) {
		this.score = score;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setLat(String lat) {
		this.lat = lat;
	}

	public void setLon(String lon) {
		this.lon = lon;
	}

	public void setText(String text) {
		this.text = text;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void updateScore() {
		// double t = (double) (total)* 100;

		// double t = (double) (score);
		double t = (double) (score) / 100;
		System.out.println("\nScore:" + t + "|total:" + total);
		realScore = (double) t / total;

		double t2 = (double) (score) / 100;
		System.out.println("badscore:" + t2 + "|badtotal:" + badTotal);
		badScore = (double) t2 / badTotal;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		score = in.readInt();
		total = in.readInt();
		badTotal = in.readInt();
		// realScore = in.readFloat();
		// id = in.readLine();
		// lat = in.readLine();
		// lon = in.readLine();
		// text = in.readLine();
		// username = in.readLine();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(score);
		out.writeInt(total);
		out.writeInt(badTotal);
		// out.writeFloat(realScore);
		// out.writeUTF(id);
		// out.writeUTF(lat);
		// out.writeUTF(lon);
		// out.writeUTF(text);
		// out.writeUTF(username);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + badTotal;
		result = prime * result + score;
		result = prime * result + total;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TweetObject other = (TweetObject) obj;
		if (badTotal != other.badTotal)
			return false;
		if (score != other.score)
			return false;
		if (total != other.total)
			return false;
		return true;
	}

	@Override
	public String toString() {
		// return "TweetObject [score=" + score + ", id=" + id + ", lat=" + lat
		// + ", lon=" + lon
		// + ", text=" + text + ", username=" + username + "]";
		JSONObject j = new JSONObject();
		updateScore();
		j.put("score", score);
		j.put("total", total);
		j.put("realScore", realScore);
		j.put("badScore", badScore);
		return j.toJSONString();
	}
}
