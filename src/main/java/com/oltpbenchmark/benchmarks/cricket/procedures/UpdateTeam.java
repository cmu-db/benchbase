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
 * UpdateTeam Procedure - Updates random team attributes
 *
 * <p>Transaction Logic: 1. Update random team details (excluding primary key) 2. Fields updated:
 * homepage, leagueid, divisionid, active status, etc.
 */
public class UpdateTeam extends Procedure {

  // SQL Statement for updating team details
  public final SQLStmt UpdateTeamDetails =
      new SQLStmt(
          "UPDATE "
              + CricketConstants.TABLENAME_TEAMS
              + " SET homepage = ?, leagueid = ?, divisionid = ?, active = ?, "
              + "homevenueid = ?, mlbamteamname = ?, mlblevel = ? "
              + "WHERE teamid = ?");

  /**
   * Execute the UpdateTeam transaction
   *
   * @param conn Database connection
   * @param teamId The team ID
   * @param workerId The worker ID for logging purposes
   * @return Number of records affected (for success indication)
   * @throws SQLException
   */
  public int run(Connection conn, int teamId, int workerId) throws SQLException {
    return updateTeamDetails(conn, teamId);
  }

  /** Update team details with random values */
  private int updateTeamDetails(Connection conn, int teamId) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(UpdateTeamDetails.getSQL())) {
      Random rand = ThreadLocalRandom.current();

      // Generate random values for team updates
      String newHomepage = "https://updated-team" + teamId + ".cricket.com";
      int newLeagueId = rand.nextInt(5) + 1; // 1-5
      int newDivisionId = rand.nextInt(3) + 1; // 1-3
      boolean newActiveStatus = rand.nextBoolean();
      int newHomeVenueId = rand.nextInt(100) + 1; // 1-100
      String newMlbamTeamName = "Updated Team " + teamId + " Official";
      String newMlbLevel = "L" + (rand.nextInt(3) + 1); // L1, L2, L3

      // Set parameters
      stmt.setString(1, newHomepage);
      stmt.setInt(2, newLeagueId);
      stmt.setInt(3, newDivisionId);
      stmt.setBoolean(4, newActiveStatus);
      stmt.setInt(5, newHomeVenueId);
      stmt.setString(6, newMlbamTeamName);
      stmt.setString(7, newMlbLevel);
      stmt.setInt(8, teamId); // WHERE clause

      return stmt.executeUpdate();
    }
  }
}
