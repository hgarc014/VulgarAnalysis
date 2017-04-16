import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Stream;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.opencsv.CSVReader;

public class ScoreTweets {
	private Cluster cluster;
	private Session session;

	private String KEYSPACE = "Twitter";
	private String TABLE_STATES = "States";
	private String TABLE_TWEETS = "Tweets";

	public Session getSession() {
		return this.session;
	}

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(),
					host.getAddress(), host.getRack());
		}
		session = cluster.connect();
	}

	public void createSchema() {
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':3};");
		session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE_TWEETS + " ("
				+ "id text PRIMARY KEY," + "text text," + "state text,"
				+ "score double,lat double,lon double, username text);");
		session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE_STATES + " ("
				+ "state text PRIMARY KEY,totalTweets bigint,badScore double,realScore double);");
		// session.execute(
		// "CREATE TABLE IF NOT EXISTS simplex.playlists (" +
		// "id uuid," +
		// "title text," +
		// "album text, " +
		// "artist text," +
		// "song_id uuid," +
		// "PRIMARY KEY (id, title, album, artist)" +
		// ");");
	}

	public void close() {
		session.close();
		cluster.close();
	}

	public void saveTweet(JSONObject j) {
		try {

			String id = j.get("id").toString();
			String text = j.get("text").toString();
			String state = j.get("state").toString();
			double score = (double) j.get("score");
			double lat = (double) j.get("lat");
			double lon = (double) j.get("long");
			String username = j.get("username").toString();

			String sql = "INSERT INTO " + KEYSPACE + "." + TABLE_TWEETS
					+ " (id,text,state,score,lat,lon,username) " + " VALUES (?,?,?,?,?,?,?);";

			PreparedStatement st = getSession().prepare(sql);
			BoundStatement bs = new BoundStatement(st);
			getSession().execute(bs.bind(id, text, state, score, lat, lon, username));

			// System.out.println("sql:" + sql);
			// session.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
			close();
		}
	}

	private Integer totalTweets = 0;
	private Integer vulgarTweets = 0;
	private double smallestScore = 1;
	private double largestScore = 0;
	private List<JSONObject> badStates = new ArrayList<>();

	private HashMap<String, String> stateName = new HashMap<>();
	private HashMap<String, Float> states = new HashMap<>();
	private HashMap<String, Float> goodWords = new HashMap<>();
	private HashMap<String, Float> badWords = new HashMap<>();

	public static void main(String[] args) {

		if (args.length != 5) {
			System.out.println("./GrabFields newDir tweetsDir stateFile badWordFile goodWordFile");
			return;
		}

		Path newDir = Paths.get(args[0]);
		Path tweetsDir = Paths.get(args[1]);
		String statesFile = args[2];
		String badWordsFile = args[3];
		String goodWordsFile = args[4];

		// GrabFields main = new GrabFields();

		ScoreTweets main = new ScoreTweets();
		main.connect("127.0.0.1");
		main.createSchema();
		// main.run(file);

		main.updateLists(statesFile, badWordsFile, goodWordsFile);
		main.execute(newDir, tweetsDir);
		main.close();

	}

	public void execute(Path newDir, Path tweetsDir) {
		int convertToMB = 1000000;
		Integer fileSize = 50 * convertToMB;
		int fileNumber = 0;
		String fileName = "tweets" + fileNumber + ".json";

		System.out.println("newDir:" + newDir);
		System.out.println("TweetDir:" + tweetsDir);

		JSONParser parser = new JSONParser();

		try (DirectoryStream<Path> stream = Files.newDirectoryStream(tweetsDir, "*.json")) {

			final Timer t = new Timer();
			TimerTask tsk = new TimerTask() {

				@Override
				public void run() {
					System.out.println("\n========================");
					System.out.println("TotalTweets:" + totalTweets);
					System.out.println("largestScore:" + largestScore);
					System.out.println("smallestScore:" + smallestScore);
					System.out.println("--------");
					System.out.println("BadTweets:" + vulgarTweets);
					System.out.println("PercentageOfBadTweets:" + (float) vulgarTweets
							/ totalTweets);
					// System.out.println("--------");
					// System.out.println("InvalidStates:" + badStates.size());
					// System.out.println("PercentageOfInvalidStates:" + (float)
					// badStates.size()
					// / totalTweets);

					System.out.println("========================\n");
				}
			};
			t.scheduleAtFixedRate(tsk, 60000, 60000);
			// FileWriter badTweets = new FileWriter(newDir + "/" +
			// "badScores.txt", true);
			for (Path tweetFile : stream) {

				Stream<String> lines = Files.lines(tweetFile);

				Scanner scn = new Scanner(tweetFile.toFile(), "UTF-8");

				Integer dont = 0;

				System.out.println("working on file" + tweetFile);

				File newTweetFile = new File(newDir + "/" + fileName);

				while (newTweetFile.length() > fileSize) {
					System.out.println(fileName + " is larger than " + fileSize / convertToMB
							+ "MB. Creating new file");
					++fileNumber;
					fileName = "tweets" + fileNumber + ".json";

					newTweetFile = new File(newDir + "/" + fileName);

				}

				FileWriter file = new FileWriter(newDir + "/" + fileName, true);

				while (scn.hasNext()) {

					String line = scn.nextLine();
					// System.out.println(line);

					JSONObject obj = null;

					try {
						obj = (JSONObject) new JSONParser().parse(line);
					} catch (ParseException e) {
						System.err.println("JSON parse error: " + e.getMessage());
						System.out.println(line);
						return;
					}
					JSONObject newObj = new JSONObject();

					String longLatObj = null;
					String lat = null;
					String lon = null;
					String fullName = null;
					String countryCode = null;
					String username = null;

					String oldFullName = null;

					Boolean isOtherTweet = false;

					Object locationObj = obj.get("geo_location");
					if (locationObj != null) {
						longLatObj = locationObj.toString();
						lat = extractFromTwitter4jObject("latitude=", longLatObj, ",");
						lon = extractFromTwitter4jObject("longitude=", longLatObj, "}");
					} else {
						isOtherTweet = true;
					}

					Object pl = obj.get("palce");
					if (isOtherTweet) {
						pl = obj.get("place");
						if (pl == null) {
							// System.out.println("place was null");
							continue;
						}
						String placeObj = pl.toString();
						countryCode = extractFromTwitter4jObject("country_code\":", placeObj, ",");
						fullName = extractFromTwitter4jObject("full_name\":", placeObj, "\",")
								+ "\"";
						oldFullName = fullName;
						String boundingBox = extractFromTwitter4jObject("bounding_box\":",
								placeObj, "}");

						JSONObject boundingboxJSON = (JSONObject) pl;
						if (boundingboxJSON.get("bounding_box") == null) {
							// System.out.println("bounding box was null");
							continue;
						}
						String coordinates = boundingboxJSON.get("bounding_box").toString();
						lon = extractFromTwitter4jObject("[[[", coordinates, ",");
						lat = extractFromTwitter4jObject(",", coordinates, "]");

						JSONObject users = (JSONObject) obj.get("user");

						username = users.get("screen_name").toString();

					} else if (pl != null) {
						String placeObj = pl.toString();
						countryCode = extractFromTwitter4jObject("countryCode=", placeObj, ",");
						fullName = extractFromTwitter4jObject("fullName=", placeObj, "',") + "'";
						oldFullName = fullName;

						String user = obj.get("user").toString();
						username = extractFromTwitter4jObject("screenName=", user, ",");

					}

					fullName = getState(fullName);
					boolean isBad = (fullName == null);
					if (isBad)
						fullName = oldFullName;

					addJSONField(obj, newObj, "id");
					addJSONField(obj, newObj, "text");
					newObj.put("state", fullName);
					newObj.put("countryCode", countryCode);

					if (newObj.get("countryCode") != null
							&& newObj.get("countryCode").toString().contains("US")) {

						newObj.put("lat", Double.parseDouble(lat));
						newObj.put("long", Double.parseDouble(lon));
						newObj.put("username", username);
						newObj.put("score", getScore(obj.get("text").toString()));

						if (isBad) {
							// JSONObject tst = newObj;
							// tst.put("oldFullName", oldFullName);
							// badStates.add(tst);
						} else {

							saveTweet(newObj);
							file.write(newObj.toJSONString() + "\n");
							// if (Float.valueOf(newObj.get("score").toString())
							// > 0f)
							// badTweets.write(newObj.toJSONString() + "\n");
						}
					} else {
						// System.out.println("Not writing countryCode not us");
					}

				}

				file.close();
				scn.close();

			}

			// HashMap<String, String> dup = new HashMap<>();
			// FileWriter fw = new FileWriter(newDir + "/badStates.txt", true);
			// for (JSONObject j : badStates) {
			// // System.out.println("------------------");
			// fw.write(j.get("fullName") + " : " + j.get("oldFullName") +
			// "\n");
			// // t.put(key, value)
			// // System.out.println("COUNTRY:")
			// }
			// fw.close();

			t.cancel();
		} catch (IOException e) {
			System.err.println("Failed to finish directory stream: " + e.getMessage());
			System.exit(1);
		}
	}

	public double getScore(String text) {
		double score = 0f;

		String[] splited = text.split(" ");

		boolean foundNegativeWord = false;
		boolean foundPositiveWord = false;

		int words = splited.length;
		float badWordCount = 0f;
		float previousWordVal = 0f;
		int multiplier = 2;

		String word = null;
		for (String myword : splited) {
			word = myword.toUpperCase();
			// is hashtag
			if (word.length() > 0) {
				if (word.charAt(0) == '#') {
					if (word.length() > 1)
						word = word.substring(1);
				}

				if (badWords.containsKey(word)) {
					// if word is bad and previous word was positive
					if (foundPositiveWord) {
						foundPositiveWord = false;
					}
					// 2 bad words in a row
					else if (foundNegativeWord) {
						badWordCount += badWords.get(word) * multiplier;
					}
					// first bad word
					else {
						badWordCount += badWords.get(word);
						previousWordVal = badWords.get(word);
						// ++badWordCount;
						foundNegativeWord = true;
					}
				} else if (goodWords.containsKey(word)) {
					// word is positive and previous word was negative
					if (foundNegativeWord) {
						// --badWordCount;
						badWordCount -= previousWordVal;
						foundNegativeWord = false;
					} else
						foundPositiveWord = true;
				}
				// if word not positive or negative reset to false
				else {
					foundNegativeWord = false;
					foundPositiveWord = false;
				}

			}

		}

		if (badWordCount > 0) {
			score = (double) badWordCount / words;
			if (score > 1)
				score = 1;
			else if (score < 0)
				score = 0;
			// System.out.println("Tweet Score:" + score);
			++vulgarTweets;
			if (score > largestScore)
				largestScore = score;
			if (score < smallestScore)
				smallestScore = score;
		}
		++totalTweets;

		return score;
	}

	private void updateLists(String statesFile, String badWordsFile, String goodWordsFile) {
		// String csvFilename = "C:\\sample.csv";
		CSVReader csvReader;
		try {
			// read states file
			csvReader = new CSVReader(new FileReader(statesFile));
			String[] row = null;
			while ((row = csvReader.readNext()) != null) {

				// System.out.println(row[0] + " # " + row[1]);

				stateName.put(row[0].toUpperCase(), row[1].toUpperCase());
				states.put(row[1].toUpperCase(), 1f);

			}
			csvReader.close();

			// read badwords
			csvReader = new CSVReader(new FileReader(badWordsFile));
			row = null;
			while ((row = csvReader.readNext()) != null) {

				badWords.put(row[0].toUpperCase(), Float.valueOf(row[1]));
			}
			csvReader.close();

			// read good words
			csvReader = new CSVReader(new FileReader(goodWordsFile));
			row = null;
			while ((row = csvReader.readNext()) != null) {

				goodWords.put(row[0].toUpperCase(), 1f);
			}
			csvReader.close();

			System.out.println("States:" + states.size());
			// System.out.println("stateNames:" + stateName.size());
			System.out.println("badwords:" + badWords.size());
			System.out.println("goodwords:" + goodWords.size());

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String getState(String fullName) {
		boolean isBad = false;
		String state = null;

		if (fullName != null) {
			if (fullName.length() >= 3)
				state = fullName.substring(fullName.length() - 3, fullName.length() - 1)
						.toUpperCase();

			if (!states.containsKey(state)) {
				if (fullName.contains(",")) {
					String stateFullName = fullName.substring(1, fullName.indexOf(','))
							.toUpperCase();

					if (stateName.containsKey(stateFullName)) {
						state = stateName.get(stateFullName);
					} else {
						isBad = true;
					}
				} else {
					isBad = true;
				}
			}

		} else
			isBad = true;

		if (isBad)
			return null;
		return state;
	}

	// public static void getJSONFields(JSONObject obj, String field) {
	// System.out.println(field + ":" + obj.get(field));
	// // return (String) obj.get(field);
	// }

	public static void addJSONField(JSONObject obj, JSONObject newObj, String field) {
		newObj.put(field, obj.get(field));
	}

	private static String extractFromTwitter4jObject(String fieldname, String t4jObj, String delimit) {
		if (t4jObj == null)
			return null;
		int start = t4jObj.indexOf(fieldname) + fieldname.length();
		int end = t4jObj.indexOf(delimit, start);

		return t4jObj.substring(start, end);
	}
}
