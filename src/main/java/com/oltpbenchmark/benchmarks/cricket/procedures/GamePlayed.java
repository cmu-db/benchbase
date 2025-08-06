package com.oltpbenchmark.benchmarks.cricket.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.cricket.CricketConstants;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GamePlayed Procedure - Main transaction for cricket benchmark
 *
 * <p>Transaction Logic: 1. Get all players for the given team 2. Find max(last_game_played) across
 * all team players 3. New game_id = max + 1 4. Insert new game record in gameDetails 5. Insert new
 * pitch record in pitchDetails 6. Update all team players' last_game_played to new game_id
 */
public class GamePlayed extends Procedure {
  private static final Logger LOG = LoggerFactory.getLogger(GamePlayed.class);

  // SQL Statements for the transaction
  public final SQLStmt GetMaxGameId =
      new SQLStmt(
          "SELECT COALESCE(MAX(last_game_played), 0) AS max_game_id FROM "
              + CricketConstants.TABLENAME_PLAYERS
              + " WHERE teamid = ?");

  public final SQLStmt InsertGame =
      new SQLStmt(
          "INSERT INTO "
              + CricketConstants.TABLENAME_GAMES
              + " (teamid, gameid, yearid, hometeamid, awayteamid, homescore, awayscore, gamedate, "
              + "gametype, gamestatus, gamenum, levelid, leagueid, venueid, venueversion, "
              + "inbats, inhd, hasplaydata, haspitchdata, isnight, isturf, lastupdatedate) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

  public final SQLStmt InsertPitch =
      new SQLStmt(
          "INSERT INTO "
              + CricketConstants.TABLENAME_PITCHES
              + " (teamid, gameid, eventseq, pitchseq, yearid, balls, strikes, result, type, "
              + "velocity, batterid, pitcherid, islastpitch, ispitch, isball, isstrike, "
              + "isswing, isinstrikezone, isofficial, pitchtotal, pitchtime) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

  public final SQLStmt UpdatePlayersLastGame =
      new SQLStmt(
          "UPDATE "
              + CricketConstants.TABLENAME_PLAYERS
              + " SET last_game_played = ? WHERE teamid = ?");

  /**
   * Execute the GamePlayed transaction
   *
   * @param conn Database connection
   * @param teamId The team ID (each worker is assigned to one team)
   * @param workerId The worker ID for logging purposes
   * @return Number of records affected (for success indication)
   * @throws SQLException
   */
  public int run(Connection conn, int teamId, int workerId) throws SQLException {
    // Step 1: Get max game ID for this team
    int maxGameId = getMaxGameIdForTeam(conn, teamId, workerId);
    int newGameId = maxGameId + 1;

    // Step 2: Insert new game record
    int gameRecordsInserted = insertGameRecord(conn, teamId, newGameId, workerId);

    // Step 3: Insert new pitch record
    int pitchRecordsInserted = insertPitchRecord(conn, teamId, newGameId, workerId);

    // Step 4: Update all players' last_game_played for this team
    int playersUpdated = updatePlayersLastGame(conn, teamId, newGameId, workerId);

    int totalRecordsAffected = gameRecordsInserted + pitchRecordsInserted + playersUpdated;

    return totalRecordsAffected;
  }

  /** Get the maximum game ID for the given team */
  private int getMaxGameIdForTeam(Connection conn, int teamId, int workerId) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(GetMaxGameId.getSQL())) {
      stmt.setInt(1, teamId);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          int maxGameId = rs.getInt("max_game_id");
          return maxGameId;
        } else {
          return 0;
        }
      }
    } catch (SQLException e) {
      throw e;
    }
  }

  /** Insert a new game record */
  private int insertGameRecord(Connection conn, int teamId, int gameId, int workerId)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(InsertGame.getSQL())) {
      Random rand = ThreadLocalRandom.current();

      stmt.setInt(1, teamId); // teamid
      stmt.setInt(2, gameId); // gameid
      stmt.setInt(3, 2024); // yearid
      stmt.setInt(4, teamId); // hometeamid (team plays at home)
      stmt.setInt(5, (teamId % CricketConstants.NUM_TEAMS) + 1); // awayteamid (another team)
      stmt.setInt(6, rand.nextInt(300) + 100); // homescore (cricket scores 100-400)
      stmt.setInt(7, rand.nextInt(300) + 100); // awayscore
      stmt.setTimestamp(8, new Timestamp(System.currentTimeMillis())); // gamedate
      stmt.setInt(9, rand.nextInt(CricketConstants.MAX_GAME_TYPE) + 1); // gametype
      stmt.setInt(10, rand.nextInt(CricketConstants.MAX_GAME_STATUS) + 1); // gamestatus
      stmt.setInt(11, 1); // gamenum
      stmt.setInt(12, rand.nextInt(5) + 1); // levelid
      stmt.setInt(13, rand.nextInt(3) + 1); // leagueid
      stmt.setInt(14, rand.nextInt(100) + 1); // venueid
      stmt.setInt(15, 1); // venueversion
      stmt.setBoolean(16, rand.nextBoolean()); // inbats
      stmt.setBoolean(17, rand.nextBoolean()); // inhd
      stmt.setBoolean(18, true); // hasplaydata
      stmt.setBoolean(19, true); // haspitchdata
      stmt.setBoolean(20, rand.nextBoolean()); // isnight
      stmt.setBoolean(21, rand.nextBoolean()); // isturf
      stmt.setTimestamp(22, new Timestamp(System.currentTimeMillis())); // lastupdatedate

      int rowsInserted = stmt.executeUpdate();
      return rowsInserted;

    } catch (SQLException e) {
      throw e;
    }
  }

  /** Insert a new pitch record */
  private int insertPitchRecord(Connection conn, int teamId, int gameId, int workerId)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(InsertPitch.getSQL())) {
      Random rand = ThreadLocalRandom.current();

      stmt.setInt(1, teamId); // teamid
      stmt.setInt(2, gameId); // gameid
      stmt.setInt(3, rand.nextInt(100) + 1); // eventseq
      stmt.setInt(4, rand.nextInt(1000) + 1); // pitchseq
      stmt.setInt(5, 2024); // yearid
      stmt.setInt(6, rand.nextInt(4)); // balls (0-3)
      stmt.setInt(7, rand.nextInt(3)); // strikes (0-2)
      stmt.setInt(8, rand.nextInt(5) + 1); // result
      stmt.setInt(9, rand.nextInt(CricketConstants.MAX_PITCH_TYPES) + 1); // type
      stmt.setBigDecimal(
          10,
          new java.math.BigDecimal(rand.nextInt(CricketConstants.MAX_VELOCITY) + 80)); // velocity
      stmt.setInt(11, rand.nextInt(CricketConstants.NUM_PLAYERS_PER_TEAM) + 1); // batterid
      stmt.setInt(12, rand.nextInt(CricketConstants.NUM_PLAYERS_PER_TEAM) + 1); // pitcherid
      stmt.setBoolean(13, rand.nextBoolean()); // islastpitch
      stmt.setBoolean(14, true); // ispitch
      stmt.setBoolean(15, rand.nextBoolean()); // isball
      stmt.setBoolean(16, rand.nextBoolean()); // isstrike
      stmt.setBoolean(17, rand.nextBoolean()); // isswing
      stmt.setBoolean(18, rand.nextBoolean()); // isinstrikezone
      stmt.setBoolean(19, true); // isofficial
      stmt.setInt(20, rand.nextInt(150) + 1); // pitchtotal
      stmt.setTimestamp(21, new Timestamp(System.currentTimeMillis())); // pitchtime

      int rowsInserted = stmt.executeUpdate();
      return rowsInserted;

    } catch (SQLException e) {
      throw e;
    }
  }

  /** Update all players' last_game_played for the given team */
  private int updatePlayersLastGame(Connection conn, int teamId, int gameId, int workerId)
      throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(UpdatePlayersLastGame.getSQL())) {
      stmt.setInt(1, gameId); // new last_game_played value
      stmt.setInt(2, teamId); // teamid filter

      int rowsUpdated = stmt.executeUpdate();
      return rowsUpdated;

    } catch (SQLException e) {
      throw e;
    }
  }
}
