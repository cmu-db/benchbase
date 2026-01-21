package com.oltpbenchmark.benchmarks.cricket;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.cricket.procedures.DeleteGame;
import com.oltpbenchmark.benchmarks.cricket.procedures.GamePlayed;
import com.oltpbenchmark.benchmarks.cricket.procedures.UpdatePlayer;
import com.oltpbenchmark.benchmarks.cricket.procedures.UpdateTeam;
import com.oltpbenchmark.types.TransactionStatus;
import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CricketWorker - Executes cricket benchmark transactions
 *
 * <p>Key Features: - Each worker is assigned to exactly one team - Executes GamePlayed transactions
 * for assigned team - Extensive logging for execution flow visibility
 */
public final class CricketWorker extends Worker<CricketBenchmark> {
  private static final Logger LOG = LoggerFactory.getLogger(CricketWorker.class);

  private final CricketBenchmark benchmark;
  private final int assignedTeamId;
  private final int workerId;

  // Procedure instances
  private GamePlayed procGamePlayed;
  private DeleteGame procDeleteGame;
  private UpdateTeam procUpdateTeam;
  private UpdatePlayer procUpdatePlayer;

  /**
   * Constructor
   *
   * @param benchmarkModule The cricket benchmark instance
   * @param id Worker ID (0-indexed)
   * @param assignedTeamId The team ID this worker is assigned to (1-indexed)
   */
  public CricketWorker(CricketBenchmark benchmarkModule, int id, int assignedTeamId) {
    super(benchmarkModule, id);
    this.benchmark = benchmarkModule;
    this.workerId = id;
    this.assignedTeamId = assignedTeamId;
  }

  @Override
  protected TransactionStatus executeWork(Connection conn, TransactionType txnType)
      throws UserAbortException, SQLException {
    try {
      TransactionStatus status = null;

      switch (txnType.getName()) {
        case "GamePlayed":
          status = executeGamePlayed(conn);
          break;
        case "DeleteGame":
          status = executeDeleteGame(conn);
          break;
        case "UpdateTeam":
          status = executeUpdateTeam(conn);
          break;
        case "UpdatePlayer":
          status = executeUpdatePlayer(conn);
          break;

        default:
          throw new RuntimeException("Unknown transaction type: " + txnType.getName());
      }

      return status;

    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error in transaction", e);
    }
  }

  /**
   * Execute GamePlayed transaction
   *
   * <p>Transaction Logic: 1. Get all players for the assigned team 2. Find max(last_game_played)
   * across all players 3. New game_id = max + 1 4. Insert new game record 5. Insert new pitch
   * record 6. Update all players' last_game_played to new game_id
   */
  private TransactionStatus executeGamePlayed(Connection conn) throws SQLException {
    try {
      // Initialize procedure if not already done
      if (this.procGamePlayed == null) {
        this.procGamePlayed = this.getProcedure(GamePlayed.class);
      }

      // Execute the game played transaction for the assigned team
      int result = this.procGamePlayed.run(conn, this.assignedTeamId, this.workerId);
      return TransactionStatus.SUCCESS;

    } catch (SQLException e) {
      throw e;
    }
  }

  private TransactionStatus executeDeleteGame(Connection conn) throws SQLException {
    try {
      // Initialize procedure if not already done
      if (this.procDeleteGame == null) {
        this.procDeleteGame = this.getProcedure(DeleteGame.class);
      }

      // Execute the delete game transaction for the assigned team
      int result = this.procDeleteGame.run(conn, this.assignedTeamId, this.workerId);
      return TransactionStatus.SUCCESS;

    } catch (SQLException e) {
      throw e;
    }
  }

  private TransactionStatus executeUpdateTeam(Connection conn) throws SQLException {
    try {
      // Initialize procedure if not already done
      if (this.procUpdateTeam == null) {
        this.procUpdateTeam = this.getProcedure(UpdateTeam.class);
      }

      // Execute the update team transaction for the assigned team
      int result = this.procUpdateTeam.run(conn, this.assignedTeamId, this.workerId);
      return TransactionStatus.SUCCESS;

    } catch (SQLException e) {
      throw e;
    }
  }

  private TransactionStatus executeUpdatePlayer(Connection conn) throws SQLException {
    try {
      // Initialize procedure if not already done
      if (this.procUpdatePlayer == null) {
        this.procUpdatePlayer = this.getProcedure(UpdatePlayer.class);
      }

      // Execute the update player transaction for the assigned team
      int result = this.procUpdatePlayer.run(conn, this.assignedTeamId, this.workerId);
      return TransactionStatus.SUCCESS;

    } catch (SQLException e) {
      throw e;
    }
  }

  /** Get the assigned team ID for this worker */
  public int getAssignedTeamId() {
    return this.assignedTeamId;
  }

  /** Get worker info string for logging and debugging */
  @Override
  public String toString() {
    return String.format(
        "CricketWorker{workerId=%d, assignedTeamId=%d}", this.workerId, this.assignedTeamId);
  }
}
