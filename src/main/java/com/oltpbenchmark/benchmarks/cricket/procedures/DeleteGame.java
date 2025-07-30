package com.oltpbenchmark.benchmarks.cricket.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.cricket.CricketConstants;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DeleteGame Procedure - Deletes the latest game for a team
 *
 * <p>Transaction Logic: 1. Get max(last_game_played) for the team - must be > 1 2. Delete game
 * record from gameDetails 3. Delete pitch record from pitchDetails 4. Update all team players'
 * last_game_played by reducing by 1
 */
public class DeleteGame extends Procedure {

  // SQL Statements for the transaction
  public final SQLStmt GetMaxGameId =
      new SQLStmt(
          "SELECT COALESCE(MAX(last_game_played), 0) AS max_game_id FROM "
              + CricketConstants.TABLENAME_PLAYERS
              + " WHERE teamid = ?");

  public final SQLStmt DeleteGameRecord =
      new SQLStmt(
          "DELETE FROM " + CricketConstants.TABLENAME_GAMES + " WHERE teamid = ? AND gameid = ?");

  public final SQLStmt DeletePitchRecord =
      new SQLStmt(
          "DELETE FROM " + CricketConstants.TABLENAME_PITCHES + " WHERE teamid = ? AND gameid = ?");

  public final SQLStmt UpdatePlayersLastGame =
      new SQLStmt(
          "UPDATE "
              + CricketConstants.TABLENAME_PLAYERS
              + " SET last_game_played = last_game_played - 1 WHERE teamid = ?");

  /**
   * Execute the DeleteGame transaction
   *
   * @param conn Database connection
   * @param teamId The team ID
   * @param workerId The worker ID for logging purposes
   * @return Number of records affected (for success indication)
   * @throws SQLException
   */
  public int run(Connection conn, int teamId, int workerId) throws SQLException {
    // Step 1: Get max game ID for this team
    int maxGameId = getMaxGameIdForTeam(conn, teamId);

    // Only proceed if there are games to delete (max > 1 to keep last_game_played > 0)
    if (maxGameId <= 1) {
      return 0; // No deletion performed
    }

    // Step 2: Delete game record
    int gameRecordsDeleted = deleteGameRecord(conn, teamId, maxGameId);

    // Step 3: Delete pitch record
    int pitchRecordsDeleted = deletePitchRecord(conn, teamId, maxGameId);

    // Step 4: Update all players' last_game_played by reducing by 1
    int playersUpdated = updatePlayersLastGame(conn, teamId);

    int totalRecordsAffected = gameRecordsDeleted + pitchRecordsDeleted + playersUpdated;
    return totalRecordsAffected;
  }

  /** Get the maximum game ID for the given team */
  private int getMaxGameIdForTeam(Connection conn, int teamId) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(GetMaxGameId.getSQL())) {
      stmt.setInt(1, teamId);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getInt("max_game_id");
        } else {
          return 0;
        }
      }
    }
  }

  /** Delete game record */
  private int deleteGameRecord(Connection conn, int teamId, int gameId) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(DeleteGameRecord.getSQL())) {
      stmt.setInt(1, teamId);
      stmt.setInt(2, gameId);
      return stmt.executeUpdate();
    }
  }

  /** Delete pitch record */
  private int deletePitchRecord(Connection conn, int teamId, int gameId) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(DeletePitchRecord.getSQL())) {
      stmt.setInt(1, teamId);
      stmt.setInt(2, gameId);
      return stmt.executeUpdate();
    }
  }

  /** Update all players' last_game_played by reducing by 1 */
  private int updatePlayersLastGame(Connection conn, int teamId) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(UpdatePlayersLastGame.getSQL())) {
      stmt.setInt(1, teamId);
      return stmt.executeUpdate();
    }
  }
}
