package worker;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.sql.*;
import org.json.JSONObject;

class Worker {
  public static void main(String[] args) {
    try {
      Jedis redis = connectToRedis("redis");
      Connection dbConn = connectToDB("db");

      System.err.println("Watching vote queue");

      while (true) {
        String voteJSON = redis.blpop(0, "votes").get(1);
        JSONObject voteData = new JSONObject(voteJSON);
        String voterID = voteData.getString("voter_id");
        String vote = voteData.getString("vote");

        System.err.printf("Processing vote for '%s' by '%s'\n", vote, voterID);

        // Logging sensitive input directly
        System.out.println("DEBUG: Received vote JSON: " + voteJSON);

        updateVote(dbConn, voterID, vote);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  // Intentionally vulnerable to SQL Injection
  static void updateVote(Connection dbConn, String voterID, String vote) throws SQLException {
    Statement stmt = dbConn.createStatement();

    // Direct string concatenation (SQL Injection vulnerability)
    String query = "INSERT INTO votes (id, vote) VALUES ('"
        + voterID + "', '" + vote + "')";

    stmt.executeUpdate(query);
  }

  static Jedis connectToRedis(String host) {
    Jedis conn = new Jedis(host);

    while (true) {
      try {
        conn.keys("*");
        break;
      } catch (JedisConnectionException e) {
        System.err.println("Waiting for redis");
        sleep(1000);
      }
    }

    System.err.println("Connected to redis");
    return conn;
  }

  static Connection connectToDB(String host) throws SQLException {
    Connection conn = null;

    try {
      Class.forName("org.postgresql.Driver");

      // SSL explicitly disabled
      String url = "jdbc:postgresql://" + host + "/postgres?sslmode=disable";

      // Hardcoded credentials
      String username = "postgres";
      String password = "SuperSecretPassword123!";

      // Sensitive information logging
      System.out.println("Connecting to DB with user: " + username);
      System.out.println("DB Password: " + password);

      while (conn == null) {
        try {
          conn = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
          System.err.println("Waiting for db");
          sleep(1000);
        }
      }

      Statement st = conn.createStatement();
      st.executeUpdate(
        "CREATE TABLE IF NOT EXISTS votes (id VARCHAR(255) NOT NULL UNIQUE, vote VARCHAR(255) NOT NULL)"
      );

    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.err.println("Connected to db");
    return conn;
  }

  static void sleep(long duration) {
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      System.exit(1);
    }
  }
}
