package com.oltpbenchmark.benchmarks.cricket.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.cricket.CricketConstants;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * UpdatePlayer Procedure - Updates random player attributes
 *
 * <p>Transaction Logic: 1. Randomly choose a few players from the team 2. Update their attributes
 * (excluding primary keys and last_game_played) 3. Fields updated: height, weight, position, active
 * status, etc.
 */
public class UpdatePlayer extends Procedure {

  // SQL Statement for updating player details
  public final SQLStmt UpdatePlayerDetails =
      new SQLStmt(
          "UPDATE "
              + CricketConstants.TABLENAME_PLAYERS
              + " SET height = ?, weight = ?, position = ?, active = ?, "
              + "college = ?, number = ?, rosterstate = ?, biourl = ? "
              + "WHERE teamid = ? AND playerid = ?");

  /**
   * Execute the UpdatePlayer transaction
   *
   * @param conn Database connection
   * @param teamId The team ID
   * @param workerId The worker ID for logging purposes
   * @return Number of records affected (for success indication)
   * @throws SQLException
   */
  public int run(Connection conn, int teamId, int workerId) throws SQLException {
    Random rand = ThreadLocalRandom.current();

    // Randomly choose 2-5 players to update from the team (playerid 1-11)
    int numPlayersToUpdate = rand.nextInt(4) + 2; // 2-5 players
    int totalUpdated = 0;

    for (int i = 0; i < numPlayersToUpdate; i++) {
      int randomPlayerId = rand.nextInt(CricketConstants.NUM_PLAYERS_PER_TEAM) + 1; // 1-11
      totalUpdated += updatePlayerDetails(conn, teamId, randomPlayerId);
    }

    return totalUpdated;
  }

  /** Update player details with random values */
  private int updatePlayerDetails(Connection conn, int teamId, int playerId) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(UpdatePlayerDetails.getSQL())) {
      Random rand = ThreadLocalRandom.current();

      // Generate random values for player updates
      int newHeight = 165 + rand.nextInt(25); // 165-189 cm
      int newWeight = 60 + rand.nextInt(40); // 60-99 kg
      int newPosition = rand.nextInt(11) + 1; // 1-11 cricket positions
      boolean newActiveStatus = rand.nextBoolean();
      String newCollege = "Updated Cricket University " + rand.nextInt(10);
      int newNumber = rand.nextInt(99) + 1; // 1-99
      int newRosterState = rand.nextInt(5) + 1; // 1-5
      String newBioUrl = "https://updated-player" + teamId + "-" + playerId + ".bio.com";

      // Set parameters
      stmt.setInt(1, newHeight);
      stmt.setInt(2, newWeight);
      stmt.setInt(3, newPosition);
      stmt.setBoolean(4, newActiveStatus);
      stmt.setString(5, newCollege);
      stmt.setInt(6, newNumber);
      stmt.setInt(7, newRosterState);
      stmt.setString(8, newBioUrl);
      stmt.setInt(9, teamId); // WHERE clause - teamid
      stmt.setInt(10, playerId); // WHERE clause - playerid

      return stmt.executeUpdate();
    }
  }
}
