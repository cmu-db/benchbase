# Cricket Benchmark Documentation

## Overview

The Cricket Benchmark is a custom database workload designed to simulate operations in a cricket sports management system. It models teams, players, games, and detailed pitch-by-pitch data with realistic cricket-specific transaction patterns.

## Database Schema

### 1. Core Tables

#### `teamDetails` (Primary Entity)
```sql
CREATE TABLE teamDetails (
    teamid INTEGER NOT NULL PRIMARY KEY,
    city VARCHAR(50),
    name VARCHAR(50),
    abbr VARCHAR(5),
    leagueid SMALLINT,
    divisionid SMALLINT,
    organizationid SMALLINT,
    homepage VARCHAR(200),
    homevenueid INTEGER,
    active BOOLEAN,
    mlbamteamname VARCHAR(80),
    mlblevel VARCHAR(2),
    -- ... additional metadata fields
);
```
- **Purpose**: Stores cricket team information
- **Key Fields**: `teamid` (PK), `city`, `name`, `abbr`, `active`
- **Capacity**: 10 teams per benchmark instance

#### `playerDetails` (Team Members)
```sql
CREATE TABLE playerDetails (
    teamid INTEGER NOT NULL,
    playerid INTEGER NOT NULL,
    lastname VARCHAR(40),
    firstname VARCHAR(40),
    height SMALLINT,
    weight SMALLINT,
    position SMALLINT,
    active BOOLEAN,
    college VARCHAR(80),
    number SMALLINT,
    rosterstate SMALLINT,
    biourl VARCHAR(200),
    last_game_played INTEGER DEFAULT 0,  -- Cricket-specific field
    -- ... extensive player metadata
    CONSTRAINT pk_playerDetails PRIMARY KEY (teamid, playerid),
    CONSTRAINT fk_playerDetails_team FOREIGN KEY (teamid) REFERENCES teamDetails(teamid)
);
```
- **Purpose**: Stores player details and their game participation
- **Key Fields**: `teamid` + `playerid` (composite PK), `last_game_played` (critical for transactions)
- **Capacity**: 11 players per team (110 total players)
- **Special Feature**: `last_game_played` tracks player activity and enables safe deletion logic

#### `gameDetails` (Match Records)
```sql
CREATE TABLE gameDetails (
    teamid INTEGER NOT NULL,
    gameid INTEGER NOT NULL,
    yearid SMALLINT,
    hometeamid SMALLINT,
    awayteamid SMALLINT,
    homescore SMALLINT,
    awayscore SMALLINT,
    gamedate TIMESTAMP,
    gametype SMALLINT,
    gamestatus SMALLINT,
    venueid INTEGER,
    -- ... extensive game metadata
    CONSTRAINT pk_gameDetails PRIMARY KEY (teamid, gameid),
    CONSTRAINT fk_gameDetails_team FOREIGN KEY (teamid) REFERENCES teamDetails(teamid)
);
```
- **Purpose**: Records individual cricket matches
- **Key Fields**: `teamid` + `gameid` (composite PK), `gamedate`, scores
- **Relationship**: Each team maintains its own game sequence

#### `pitchDetails` (Ball-by-Ball Data)
```sql
CREATE TABLE pitchDetails (
    teamid INTEGER NOT NULL,
    gameid INTEGER NOT NULL,
    eventseq SMALLINT,
    pitchseq INTEGER,
    yearid SMALLINT,
    balls SMALLINT,
    strikes SMALLINT,
    result SMALLINT,
    type SMALLINT,
    velocity DECIMAL(6,3),
    batterid INTEGER,
    pitcherid INTEGER,
    -- ... extensive pitch analysis data
    CONSTRAINT pk_pitchDetails PRIMARY KEY (teamid, gameid)
);
```
- **Purpose**: Stores detailed pitch-by-pitch cricket data
- **Key Fields**: `teamid` + `gameid` (composite PK), `velocity`, `type`, `result`
- **Analytics**: Supports detailed performance analysis and statistics

### 2. Relationships & Constraints

```
teamDetails (1) ──── (N) playerDetails
     │
     └── (1) ──── (N) gameDetails (1) ──── (1) pitchDetails
```

- **Team-centric design**: All data partitioned by `teamid`
- **Referential integrity**: Foreign key constraints ensure data consistency
- **Performance indexes**: Optimized for team-based queries and game sequence access

## Transaction Procedures

### 1. GamePlayed (Primary Transaction - 60% weight)

**Purpose**: Simulates a cricket match being played and recorded

**Operations**:
1. Determine next game ID for the team
2. Insert new game record with random match data
3. Insert corresponding pitch details with realistic cricket metrics
4. Update all team players' `last_game_played` counter

**Business Logic**:
- Creates realistic cricket match data (scores, venue, date)
- Generates pitch-by-pitch details (velocity, type, result)
- Maintains player participation tracking
- Ensures data consistency across all related tables

### 2. DeleteGame (Cleanup Transaction - 20% weight)

**Purpose**: Removes the most recent game and maintains data integrity

**Operations**:
1. **Safety Check**: Verify `max(last_game_played) > 1` to prevent deletion of last game
2. Delete latest game record from `gameDetails`
3. Delete corresponding pitch data from `pitchDetails`
4. Decrement all team players' `last_game_played` by 1

**Safety Features**:
- **Data Protection**: Never deletes if it would leave players with `last_game_played = 0`
- **Referential Integrity**: Cascading deletion of related pitch data
- **Atomic Operation**: All-or-nothing transaction semantics

### 3. UpdateTeam (Team Management - 10% weight)

**Purpose**: Simulates team information updates and administrative changes

**Operations**:
- Updates non-critical team attributes with random values
- Fields modified: `homepage`, `leagueid`, `divisionid`, `active`, `homevenueid`, `mlbamteamname`, `mlblevel`

**Safety Features**:
- **Primary Key Protection**: Never modifies `teamid`
- **Random Data Generation**: Realistic value ranges for each field
- **Single Row Updates**: Targets specific team only

### 4. UpdatePlayer (Player Management - 10% weight)

**Purpose**: Simulates player roster updates and attribute modifications

**Operations**:
1. Randomly select 2-5 players from the team
2. Update their physical and administrative attributes
3. Fields modified: `height`, `weight`, `position`, `active`, `college`, `number`, `rosterstate`, `biourl`

**Safety Features**:
- **Key Protection**: Never modifies `teamid`, `playerid`, or `last_game_played`
- **Selective Updates**: Only affects subset of players
- **Realistic Data**: Generated values within appropriate ranges

## Configuration & Workload

### Transaction Weights
```xml
<weights>60,20,10,10</weights>
```
- **GamePlayed**: 60% - Primary game simulation
- **DeleteGame**: 20% - Game cleanup and data management  
- **UpdateTeam**: 10% - Administrative team updates
- **UpdatePlayer**: 10% - Player roster management

### Benchmark Parameters
- **Teams**: 10 cricket teams
- **Players**: 11 players per team (110 total)
- **Terminals**: 10 (one per team)
- **Isolation**: `TRANSACTION_READ_COMMITTED`
- **Batch Size**: 128 operations

## Implementation Statistics

### Files Created/Modified

#### New Files (3):
1. **`DeleteGame.java`** - 112 lines
   - Complex transaction with safety checks
   - Multi-table operations (games, pitches, players)
   
2. **`UpdateTeam.java`** - 69 lines  
   - Team attribute management
   - Random data generation
   
3. **`UpdatePlayer.java`** - 84 lines
   - Player roster management 
   - Bulk player updates

#### Modified Files (2):
1. **`CricketWorker.java`** - Added ~60 lines
   - Transaction routing logic
   - Procedure initialization
   - Error handling enhancements
   
2. **`sample_cricket_config.xml`** - Restructured configuration
   - Multi-transaction type definitions
   - Weighted transaction distribution

### Code Statistics
- **Total New Lines**: 265 lines (new procedures)
- **Total Modified Lines**: ~70 lines (worker + config)
- **Overall Implementation**: ~335 lines of code
- **Files Touched**: 5 files total

## Performance Characteristics

### Expected Transaction Distribution
Based on 1,500 transactions over 30 seconds:
- **GamePlayed**: ~900 transactions (data creation intensive)
- **DeleteGame**: ~300 transactions (with safety early returns)
- **UpdateTeam**: ~150 transactions (lightweight updates)
- **UpdatePlayer**: ~150 transactions (moderate complexity)

### Database Growth Pattern
- **Games/Pitches**: Net positive growth (more creates than deletes)
- **Team/Player Data**: Stable size with periodic updates
- **Storage**: Linear growth primarily in `gameDetails` and `pitchDetails`

## Usage Examples

### Basic Execution
```bash
./mvnw exec:java -P postgres \
  -Dexec.args="-b cricket -c config/postgres/sample_cricket_config.xml \
               --create=true --load=true --execute=true"
```

### Custom Weight Configuration
```xml
<!-- More aggressive deletion testing -->
<weights>40,40,10,10</weights>

<!-- Update-heavy workload -->
<weights>30,10,30,30</weights>

<!-- Game-focused simulation -->
<weights>80,10,5,5</weights>
```

## Cricket Domain Modeling

The benchmark accurately reflects cricket sports management with:

### Realistic Team Structure
- **IPL-inspired teams**: Mumbai Indians, Chennai Super Kings, etc.
- **Indian cricket names**: Rohit, Virat, MS Dhoni, etc.
- **Authentic abbreviations**: MI, CSK, RCB, etc.

### Cricket-Specific Data
- **Player positions**: 1-11 (wicket keeper, batsmen, bowlers, all-rounders)
- **Pitch velocities**: 60-160 km/h (realistic bowling speeds)
- **Game types**: Various cricket formats
- **Performance metrics**: Detailed ball-by-ball analysis

### Operational Realism
- **Game sequences**: Teams play progressive matches
- **Player tracking**: Participation history maintained
- **Administrative updates**: Roster changes and team management
- **Data lifecycle**: Games created, managed, and eventually archived

This benchmark provides a comprehensive, realistic simulation of cricket database operations with proper safety mechanisms and authentic domain modeling. 