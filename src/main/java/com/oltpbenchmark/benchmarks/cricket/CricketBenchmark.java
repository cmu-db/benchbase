package com.oltpbenchmark.benchmarks.cricket;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.cricket.procedures.GamePlayed;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cricket Benchmark Implementation
 *
 * <p>Features: - 4 tables: teamDetails, playerDetails, gameDetails, pitchDetails - Each
 * terminal/thread maps to exactly one team - Pre-loads teams and players - Main transaction:
 * GamePlayed (inserts game and pitch records, updates player last_game_played)
 */
public final class CricketBenchmark extends BenchmarkModule {
  private static final Logger LOG = LoggerFactory.getLogger(CricketBenchmark.class);

  protected final int numTeams;
  protected final int numPlayersPerTeam;
  protected final int totalPlayers;

  /**
   * Constructor for Cricket Benchmark Number of teams = number of terminals/threads (as per
   * requirement)
   */
  public CricketBenchmark(WorkloadConfiguration workConf) {
    super(workConf);

    // Number of teams equals number of terminals (each terminal maps to one team)
    this.numTeams = (int) workConf.getScaleFactor();
    this.numPlayersPerTeam = CricketConstants.NUM_PLAYERS_PER_TEAM;
    this.totalPlayers = this.numTeams * this.numPlayersPerTeam;

    LOG.info("CricketBenchmark initialized with:");
    LOG.info("  - Number of Teams: {}", this.numTeams);
    LOG.info("  - Players per Team: {}", this.numPlayersPerTeam);
    LOG.info("  - Total Players: {}", this.totalPlayers);
    LOG.info("  - Number of Terminals: {}", workConf.getTerminals());
    LOG.info("  - Database Type: {}", workConf.getDatabaseType());

    // Validate that we have enough teams for terminals
    if (this.numTeams > CricketConstants.NUM_TEAMS) {
      LOG.warn(
          "Number of terminals ({}) exceeds maximum teams ({}). Some terminals will use duplicate team assignments.",
          this.numTeams,
          CricketConstants.NUM_TEAMS);
    }

    if (this.numTeams < 1) {
      throw new IllegalArgumentException("Number of teams/terminals must be at least 1");
    }
  }

  /** Create workers - each worker is assigned to exactly one team */
  @Override
  protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
    LOG.info("Creating {} cricket workers, each mapped to a specific team", this.numTeams);

    List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();

    for (int i = 0; i < this.numTeams; i++) {
      // Each worker gets assigned to a team (1-indexed team IDs)
      int assignedTeamId = (i % CricketConstants.NUM_TEAMS) + 1;
      CricketWorker worker = new CricketWorker(this, i, assignedTeamId);
      workers.add(worker);

      LOG.debug("Created worker {} assigned to team {}", i, assignedTeamId);
    }

    LOG.info("Successfully created {} cricket workers", workers.size());
    return workers;
  }

  /** Create the loader for pre-loading teams and players */
  @Override
  protected Loader<CricketBenchmark> makeLoaderImpl() {
    LOG.info("Creating cricket loader for pre-loading teams and players");
    return new CricketLoader(this);
  }

  /** Get the package containing procedure classes */
  @Override
  protected Package getProcedurePackageImpl() {
    return GamePlayed.class.getPackage();
  }

  // Getters for configuration values
  public int getNumTeams() {
    return numTeams;
  }

  public int getNumPlayersPerTeam() {
    return numPlayersPerTeam;
  }

  public int getTotalPlayers() {
    return totalPlayers;
  }

  /**
   * Get the team ID for a specific worker/terminal
   *
   * @param workerId The worker ID (0-indexed)
   * @return Team ID (1-indexed)
   */
  public int getTeamIdForWorker(int workerId) {
    return (workerId % CricketConstants.NUM_TEAMS) + 1;
  }
}
