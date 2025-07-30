package com.oltpbenchmark.benchmarks.cricket;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CricketLoader - Loads initial data for cricket benchmark
 *
 * <p>Pre-loads: - Teams (10 teams by default, configurable based on terminals) - Players (11
 * players per team with composite key teamid, playerid)
 */
public final class CricketLoader extends Loader<CricketBenchmark> {
  private static final Logger LOG = LoggerFactory.getLogger(CricketLoader.class);

  private final CricketBenchmark benchmark;

  // Table catalogs and SQL statements
  private final Table catalogTeams;
  private final Table catalogPlayers;
  private final String sqlInsertTeam;
  private final String sqlInsertPlayer;

  public CricketLoader(CricketBenchmark benchmark) {
    super(benchmark);
    this.benchmark = benchmark;

    // Get table catalogs from benchmark
    this.catalogTeams = this.benchmark.getCatalog().getTable(CricketConstants.TABLENAME_TEAMS);
    this.catalogPlayers = this.benchmark.getCatalog().getTable(CricketConstants.TABLENAME_PLAYERS);

    // Generate SQL INSERT statements using catalog
    this.sqlInsertTeam = SQLUtil.getInsertSQL(this.catalogTeams, this.getDatabaseType());
    this.sqlInsertPlayer = SQLUtil.getInsertSQL(this.catalogPlayers, this.getDatabaseType());

    LOG.info(
        "CricketLoader initialized for {} teams with {} players per team",
        benchmark.getNumTeams(),
        benchmark.getNumPlayersPerTeam());
    LOG.debug("Team INSERT SQL: {}", this.sqlInsertTeam);
    LOG.debug("Player INSERT SQL: {}", this.sqlInsertPlayer);
  }

  @Override
  public List<LoaderThread> createLoaderThreads() throws SQLException {
    List<LoaderThread> loaderThreads = new ArrayList<>();

    LOG.info(
        "Creating loader threads for {} teams and {} total players",
        benchmark.getNumTeams(),
        benchmark.getTotalPlayers());

    // Create one thread for teams, one for players
    loaderThreads.add(new TeamLoader());
    loaderThreads.add(new PlayerLoader());

    return loaderThreads;
  }

  /** LoaderThread for loading teams */
  private class TeamLoader extends LoaderThread {
    public TeamLoader() {
      super(CricketLoader.this.benchmark);
    }

    @Override
    public void load(Connection conn) throws SQLException {
      LOG.info("Loading {} teams...", benchmark.getNumTeams());

      try (PreparedStatement stmt = conn.prepareStatement(CricketLoader.this.sqlInsertTeam)) {

        for (int teamId = 1; teamId <= benchmark.getNumTeams(); teamId++) {
          setTeamParameters(stmt, teamId);
          stmt.addBatch();

          if (teamId % 100 == 0) {
            stmt.executeBatch();
            LOG.debug("Loaded {} teams so far...", teamId);
          }
        }

        // Execute remaining batches
        stmt.executeBatch();

        LOG.info("Successfully loaded {} teams", benchmark.getNumTeams());

      } catch (SQLException ex) {
        LOG.error("Failed to load teams", ex);
        throw new RuntimeException("Error loading teams", ex);
      }
    }

    /** Set parameters for team insertion */
    private void setTeamParameters(PreparedStatement stmt, int teamId) throws SQLException {
      Random rand = ThreadLocalRandom.current();
      int cityIndex = (teamId - 1) % CricketConstants.TEAM_CITIES.length;

      stmt.setInt(1, teamId); // teamid
      stmt.setString(2, CricketConstants.TEAM_CITIES[cityIndex]); // city
      stmt.setString(3, CricketConstants.TEAM_NAMES[cityIndex]); // name
      stmt.setString(4, CricketConstants.TEAM_ABBRS[cityIndex]); // abbr
      stmt.setInt(5, rand.nextInt(5) + 1); // leagueid
      stmt.setInt(6, rand.nextInt(3) + 1); // divisionid
      stmt.setInt(7, rand.nextInt(10) + 1); // organizationid
      stmt.setInt(8, rand.nextInt(5) + 1); // levelid
      stmt.setString(9, "https://team" + teamId + ".cricket.com"); // homepage
      stmt.setInt(10, rand.nextInt(100) + 1); // homevenueid
      stmt.setInt(11, teamId + 1000); // statsteamid
      stmt.setInt(12, teamId + 2000); // mlbamteamid
      stmt.setString(13, CricketConstants.TEAM_NAMES[cityIndex] + " Official"); // mlbamteamname
      stmt.setString(14, CricketConstants.TEAM_ABBRS[cityIndex]); // mlbclub
      stmt.setString(15, "C" + (teamId % 10)); // mlborg (max 3 chars)
      stmt.setString(16, "L" + (rand.nextInt(3) + 1)); // mlblevel
      stmt.setInt(17, teamId + 3000); // npbteamid
      stmt.setInt(18, teamId + 4000); // boydteamid
      stmt.setString(19, "CS" + String.format("%03d", teamId)); // csteamid
      stmt.setInt(20, teamId + 5000); // bisteamid
      stmt.setInt(21, teamId + 6000); // howeteamid
      stmt.setInt(22, teamId + 7000); // pointstreakteamid
      stmt.setString(23, "TM" + String.format("%05d", teamId)); // trackmanteamid
      stmt.setInt(24, teamId + 8000); // sportvisionteamid
      stmt.setInt(25, teamId + 9000); // ncaateamid
      stmt.setInt(26, teamId + 10000); // schoolid
      stmt.setBoolean(27, true); // active
      stmt.setString(28, "CW" + String.format("%04d", teamId)); // cartwrightteamid
      stmt.setInt(29, teamId + 11000); // kboteamid
      stmt.setInt(30, teamId + 12000); // onitteamid
      stmt.setInt(31, teamId + 13000); // fieldfxteamid
    }
  }

  /** LoaderThread for loading players */
  private class PlayerLoader extends LoaderThread {
    public PlayerLoader() {
      super(CricketLoader.this.benchmark);
    }

    @Override
    public void load(Connection conn) throws SQLException {
      LOG.info(
          "Loading {} players across {} teams...",
          benchmark.getTotalPlayers(),
          benchmark.getNumTeams());

      try (PreparedStatement stmt = conn.prepareStatement(CricketLoader.this.sqlInsertPlayer)) {

        int totalPlayersLoaded = 0;

        for (int teamId = 1; teamId <= benchmark.getNumTeams(); teamId++) {
          for (int playerId = 1; playerId <= benchmark.getNumPlayersPerTeam(); playerId++) {
            setPlayerParameters(stmt, teamId, playerId);
            stmt.addBatch();
            totalPlayersLoaded++;

            if (totalPlayersLoaded % 500 == 0) {
              stmt.executeBatch();
              LOG.debug("Loaded {} players so far...", totalPlayersLoaded);
            }
          }
        }

        // Execute remaining batches
        stmt.executeBatch();

        LOG.info("Successfully loaded {} players", totalPlayersLoaded);

      } catch (SQLException ex) {
        LOG.error("Failed to load players", ex);
        throw new RuntimeException("Error loading players", ex);
      }
    }

    /** Set parameters for player insertion */
    private void setPlayerParameters(PreparedStatement stmt, int teamId, int playerId)
        throws SQLException {
      Random rand = ThreadLocalRandom.current();
      int firstNameIndex = rand.nextInt(CricketConstants.FIRST_NAMES.length);
      int lastNameIndex = rand.nextInt(CricketConstants.LAST_NAMES.length);

      stmt.setInt(1, teamId); // teamid
      stmt.setInt(2, playerId); // playerid
      stmt.setString(3, CricketConstants.LAST_NAMES[lastNameIndex]); // lastname
      stmt.setString(4, CricketConstants.FIRST_NAMES[firstNameIndex]); // firstname
      stmt.setString(5, null); // middlename
      stmt.setString(6, CricketConstants.FIRST_NAMES[firstNameIndex]); // usesname
      stmt.setInt(7, rand.nextInt(3) + 1); // bats (1=Right, 2=Left, 3=Switch)
      stmt.setInt(8, rand.nextInt(3) + 1); // throws (1=Right, 2=Left, 3=Switch)
      stmt.setInt(9, rand.nextInt(100) + 1); // agentid
      stmt.setInt(10, rand.nextInt(50) + 1); // agencyid
      stmt.setInt(11, rand.nextInt(10) + 1); // organizationid
      stmt.setInt(12, 2015 + rand.nextInt(10)); // firstyear
      stmt.setInt(13, 2020 + rand.nextInt(5)); // lastyear
      stmt.setTimestamp(
          14,
          new Timestamp(
              System.currentTimeMillis()
                  - rand.nextInt(365 * 10) * 24L * 60 * 60 * 1000)); // birthdate
      stmt.setString(15, "Mumbai"); // birthcity
      stmt.setString(16, "India"); // birthcountry
      stmt.setString(17, "Maharashtra"); // birthstate
      stmt.setString(18, "Cricket University"); // college
      stmt.setBoolean(19, rand.nextBoolean()); // isamateur
      stmt.setInt(20, rand.nextInt(5) + 1); // class
      stmt.setInt(21, 165 + rand.nextInt(25)); // height (cm)
      stmt.setInt(22, 60 + rand.nextInt(40)); // weight (kg)
      stmt.setBoolean(23, true); // active
      stmt.setInt(24, rand.nextInt(5) + 1); // fatype
      stmt.setBoolean(25, rand.nextBoolean()); // ison40manroster
      stmt.setBoolean(26, rand.nextBoolean()); // ebispotentialmnfa
      stmt.setInt(27, rand.nextInt(11) + 1); // position (1-11 cricket positions)
      stmt.setInt(28, rand.nextInt(11) + 1); // rosterposition
      stmt.setInt(29, rand.nextInt(11) + 1); // primaryposition
      stmt.setInt(30, rand.nextInt(99) + 1); // number
      stmt.setBigDecimal(31, null); // openingdaymjservice
      stmt.setBigDecimal(32, null); // mjservice
      stmt.setBigDecimal(33, null); // projmjservice
      stmt.setBoolean(34, rand.nextBoolean()); // establishedmlb
      stmt.setInt(35, rand.nextInt(3)); // optionsused
      stmt.setInt(36, rand.nextInt(5) + 1); // optionstotal
      stmt.setBoolean(37, rand.nextBoolean()); // outofoptions
      stmt.setBoolean(38, rand.nextBoolean()); // hadprioroutright
      stmt.setBoolean(39, rand.nextBoolean()); // isr5elig
      stmt.setInt(40, 2020 + rand.nextInt(5)); // r5eligyear
      stmt.setInt(41, 2020 + rand.nextInt(5)); // mnfayear
      stmt.setInt(42, rand.nextInt(5) + 1); // rosterstate
      stmt.setInt(43, rand.nextInt(1000) + 1); // mlbdraftnumber
      stmt.setTimestamp(44, null); // waiversrequireddate
      stmt.setInt(45, rand.nextInt(30)); // assignmentoptiondays
      stmt.setTimestamp(46, null); // prioroutrightdate
      stmt.setInt(47, rand.nextInt(10) + 1); // mncontractyears
      stmt.setInt(48, rand.nextInt(10) + 1); // mnserviceyears
      stmt.setInt(49, rand.nextInt(5) + 1); // mnrosterstatus
      stmt.setInt(50, rand.nextInt(5) + 1); // mjrosterstatus
      stmt.setTimestamp(51, new Timestamp(System.currentTimeMillis())); // lastupdatedate
      stmt.setInt(52, teamId + 10000); // schoolid
      stmt.setInt(53, 2020 + rand.nextInt(5)); // r4eligyear
      stmt.setString(54, "https://player" + teamId + "-" + playerId + ".bio.com"); // biourl
      stmt.setString(
          55, "https://player" + teamId + "-" + playerId + ".headshot.jpg"); // headshoturl
      stmt.setBoolean(56, rand.nextBoolean()); // isinternational
      stmt.setInt(57, rand.nextInt(50) + 1); // countryid
      stmt.setInt(58, 2020 + rand.nextInt(5)); // mjfayear
      stmt.setBoolean(59, rand.nextBoolean()); // isr5selection
      stmt.setInt(60, 2020 + rand.nextInt(5)); // inteligyear
      stmt.setInt(61, rand.nextInt(100) + 1); // coachid
      stmt.setBoolean(62, rand.nextBoolean()); // isactivecoach
      stmt.setInt(63, rand.nextInt(3) + 1); // twoway
      stmt.setString(
          64,
          CricketConstants.FIRST_NAMES[firstNameIndex]
              + " "
              + CricketConstants.LAST_NAMES[lastNameIndex]); // search_column
      stmt.setInt(65, rand.nextInt(3)); // fourthoptionyears
      stmt.setString(66, "2021,2022"); // fourthoptionyearslist
      stmt.setString(67, "2020,2021"); // optionyearschargedlist
      stmt.setInt(68, rand.nextInt(180)); // optiondayscurrseason
      stmt.setInt(69, rand.nextInt(3)); // currentyearoptions
      stmt.setInt(70, rand.nextInt(162)); // currentseasonmlsdays
      stmt.setInt(
          71,
          CricketConstants
              .INITIAL_LAST_GAME_PLAYED); // last_game_played - CRITICAL for transactions
    }
  }
}
