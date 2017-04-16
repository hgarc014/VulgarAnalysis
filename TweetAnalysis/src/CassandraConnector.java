import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.stream.Stream;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraConnector {
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
				+ "id uuid PRIMARY KEY," + "text text," + "state text,"
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

	public void loadData() {
		// session.execute(
		// "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
		// "VALUES (" +
		// "756716f7-2e54-4715-9f00-91dcbea6cf50," +
		// "'La Petite Tonkinoise'," +
		// "'Bye Bye Blackbird'," +
		// "'Joséphine Baker'," +
		// "{'jazz', '2013'})" +
		// ";");
		// session.execute("INSERT INTO simplex.playlists (id, song_id, title, album, artist) "
		// + "VALUES (" + "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,"
		// + "756716f7-2e54-4715-9f00-91dcbea6cf50," + "'La Petite Tonkinoise',"
		// + "'Bye Bye Blackbird'," + "'Joséphine Baker'" + ");");
	}

	public void querySchema() {
		// ResultSet results =
		// session.execute("SELECT * FROM simplex.playlists " +
		// "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		// System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title",
		// "album", "artist",
		// "-------------------------------+-----------------------+--------------------"));
		// for (Row row : results) {
		// System.out.println(String.format("%-30s\t%-20s\t%-20s",
		// row.getString("title"),
		// row.getString("album"), row.getString("artist")));
		// }
		// System.out.println();
	}

	public void close() {
		session.close();
		cluster.close();
	}

	public void saveTweet(JSONObject j) {
		try {
			String state = j.get("state").toString();
			long totalTweets = (long) j.get("total");
			double badScore = (double) j.get("badScore");
			double realScore = (double) j.get("realScore");
			String sql = "INSERT INTO " + KEYSPACE + "." + TABLE_STATES + " ("
					+ "state,totalTweets,badScore,realScore) VALUES ('" + state + "'," + totalTweets
					+ "," + badScore + "," + realScore + ");";
			System.out.println("sql:" + sql);
			session.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
			close();
		}
	}

	public void run(String fileLocation) {
		try {

			// String fileLocation =
			// "/home/henry/Desktop/hadoop-2.7.1/output.txt";
			Path tweetFile = Paths.get(fileLocation);
			Scanner scn = new Scanner(tweetFile.toFile(), "UTF-8");

			JSONParser parser = new JSONParser();
			JSONObject obj;

			while (scn.hasNext()) {
				String line = scn.nextLine();

				String state = line.substring(0, 2);
				String jsonstr = line.substring(line.indexOf('{'));

				// System.out.println("State:" + state);
				// System.out.println("jsonstr:" + jsonstr);

				obj = (JSONObject) parser.parse(jsonstr);
				obj.put("state", state);
				// System.out.println("Saving:" + state);
				saveTweet(obj);
				// System.out.println("---------------");
				// System.out.println(state);
				// System.out.println(obj.get("score"));
				// System.out.println(obj.get("total"));
				// System.out.println(obj.get("badScore"));
				// System.out.println(obj.get("realScore"));
			}

			// String line =
			// "AK	{\"score\":164,\"total\":4064,\"badScore\":0.07454545454545454,\"realScore\":4.0354330708661414E-4}";

		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("./cassandra file");
			return;
		}
		String file = args[0];

		System.out.println("FILE:" + file);

		CassandraConnector client = new CassandraConnector();
		client.connect("127.0.0.1");
		client.createSchema();
		client.run(file);
		// client.loadData();
		// client.querySchema();
		client.close();
	}
}
