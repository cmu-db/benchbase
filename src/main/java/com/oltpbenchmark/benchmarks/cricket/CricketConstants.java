package com.oltpbenchmark.benchmarks.cricket;

public abstract class CricketConstants {

  // Table Names
  public static final String TABLENAME_TEAMS = "teamDetails";
  public static final String TABLENAME_PLAYERS = "playerDetails";
  public static final String TABLENAME_GAMES = "gameDetails";
  public static final String TABLENAME_PITCHES = "pitchDetails";

  // Pre-loading Configuration
  public static final int NUM_TEAMS = 10;
  public static final int NUM_PLAYERS_PER_TEAM = 11;
  public static final int TOTAL_PLAYERS = NUM_TEAMS * NUM_PLAYERS_PER_TEAM;

  // Initial Data Constants
  public static final int INITIAL_LAST_GAME_PLAYED = 0;

  // Sample Data for Teams
  public static final String[] TEAM_CITIES = {
    "Mumbai", "Chennai", "Kolkata", "Delhi", "Bangalore",
    "Hyderabad", "Rajasthan", "Punjab", "Gujarat", "Lucknow"
  };

  public static final String[] TEAM_NAMES = {
    "Indians", "Super Kings", "Knight Riders", "Capitals", "Royal Challengers",
    "Sunrisers", "Royals", "Kings", "Titans", "Super Giants"
  };

  public static final String[] TEAM_ABBRS = {
    "MI", "CSK", "KKR", "DC", "RCB", "SRH", "RR", "PBKS", "GT", "LSG"
  };

  // Sample Data for Players
  public static final String[] FIRST_NAMES = {
    "Rohit",
    "Virat",
    "MS",
    "Jasprit",
    "Hardik",
    "KL",
    "Shikhar",
    "Rishabh",
    "Yuzvendra",
    "Mohammed",
    "Ravindra",
    "Suryakumar",
    "Shreyas",
    "Ishan",
    "Washington",
    "Axar",
    "Deepak",
    "Bhuvneshwar",
    "Trent",
    "Kane"
  };

  public static final String[] LAST_NAMES = {
    "Sharma", "Kohli", "Dhoni", "Bumrah", "Pandya", "Rahul", "Dhawan", "Pant",
    "Chahal", "Shami", "Jadeja", "Yadav", "Iyer", "Kishan", "Sundar", "Patel",
    "Chahar", "Kumar", "Boult", "Williamson"
  };

  // Game and Pitch Data Constants
  public static final int MAX_GAME_TYPE = 5;
  public static final int MAX_GAME_STATUS = 3;
  public static final int MAX_PITCH_TYPES = 10;
  public static final int MAX_VELOCITY = 160;
}
