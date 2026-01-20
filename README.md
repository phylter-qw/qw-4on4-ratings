# Objectives

1. Compute regional ratings for individual players in QuakeWorld 4on4, using statistics gathered from matches played.

2. Tabulate ratings into regional rankings, updated regularly and accessible via the web. See [QuakeWorld 4on4 Ratings](https://qw-4on4-ratings.netlify.app/).

# Methodology

It is well known within the community which players have the greatest skill. Many have been playing for decades, and in that time have developed a keen sense of what it means to be a strong player, understanding which factors contribute most to individual performance.

It was hypothesized that these top players would largely agree on what those factors were; that they had conducted a kind of distributed statistical regression in human brains over many years. Does the current crop of statistics gathered after every match contain enough information to model that shared wisdom, at least in part?

To answer that question, a survey was developed asking participants to rank the importance of each post-match statistic on a number scale. The survey was sent to top players from multiple regions and the responses were averaged to derive [weights](weights.txt).

With results from the survey in hand, thousands of matches recorded on the [QuakeWorld Hub](https://hub.quakeworld.nu) were analyzed in order to calculate an approximate mean and standard deviation for every weighted statistic in each region. The raw scores for players in a match, such as the number of red armors collected, were normalized into *standard scores*, defined as the signed number of standard deviations by which the raw score deviates from the mean.

Standard scores were then combined into *match scores* for each player by taking the weighted sum, using weights derived from the survey. Finally, match scores were used to construct an input to [TrueSkill](https://trueskill.org), an algorithm developed for Xbox Live which calculates and cumulatively updates player ratings in competitive games with more than two participants.

# Implementation

The [sync.py](sync.py) script synchronizes a local [SQLite](https://sqlite.org/) database with online information about servers, players, and matches. For every 4on4 match recorded on the hub, the players, statistics, and server information are processed and stored locally in tables. The content of these tables is subsequently used to calculate regional player ratings according to the method described above.

The subsections below describe the various operations performed by the [sync.py](sync.py) script in more detail.

## Update Servers

Query [quakeservers.net](https://www.quakeservers.net) for a list of currently active server addresses. For each address returned, use reverse IP lookup to determine the global region where the address is located and query the associated server for its name. Store the results in a table with the following schema:

```sql
CREATE TABLE servers(
    server_name TEXT,
    server_region TEXT,
    PRIMARY KEY(server_name)
);
```

## Update Matches

Scrape the [QuakeWorld Hub](https://hub.quakeworld.nu) for information on all 4on4 matches played since some past cutoff date, which is usually the date of the last recorded match in the database. The end-of-match statistics for players are gathered as part of this operation. Store the results in tables with the following schemas:

```sql
CREATE TABLE matches(
    match_id INTEGER,
    match_date TEXT,
    match_tag TEXT,
    match_map TEXT,
    server_name TEXT,
    server_port INTEGER,
    match_deathmatch_mode INTEGER,
    match_teamplay_mode INTEGER,
    match_time_limit_mins INTEGER,
    match_duration_secs INTEGER,
    match_demo_sha256 TEXT,
    PRIMARY KEY(match_id)
);
CREATE TABLE players(
    match_id INTEGER,
    player_name TEXT,
    player_login TEXT,
    player_team TEXT,
    player_top_color INTEGER,
    player_bottom_color INTEGER,
    player_ping INTEGER,
    player_frags INTEGER,
    player_deaths INTEGER,
    player_teamkills INTEGER,
    player_spawnfrags INTEGER,
    player_suicides INTEGER,
    player_damage_taken INTEGER,
    player_damage_given INTEGER,
    player_damage_team INTEGER,
    player_damage_self INTEGER,
    player_damage_team_weapons INTEGER,
    player_damage_enemy_weapons INTEGER,
    player_damage_to_die INTEGER,
    player_spree_frag INTEGER,
    player_spree_quad INTEGER,
    player_speed_max REAL,
    player_speed_avg REAL,
    player_sg_attacks INTEGER,
    player_sg_hits INTEGER,
    player_sg_damage_enemy INTEGER,
    player_sg_damage_team INTEGER,
    player_ssg_attacks INTEGER,
    player_ssg_hits INTEGER,
    player_ssg_damage_enemy INTEGER,
    player_ssg_damage_team INTEGER,
    player_gl_attacks INTEGER,
    player_gl_directs INTEGER,
    player_gl_virtual INTEGER,
    player_rl_attacks INTEGER,
    player_rl_directs INTEGER,
    player_rl_virtual INTEGER,
    player_rl_dropped INTEGER,
    player_rl_taken INTEGER,
    player_rl_transfer INTEGER,
    player_rl_damage_enemy INTEGER,
    player_rl_damage_team INTEGER,
    player_rl_kills_enemy INTEGER,
    player_rl_kills_team INTEGER,
    player_lg_attacks INTEGER,
    player_lg_hits INTEGER,
    player_lg_dropped INTEGER,
    player_lg_taken INTEGER,
    player_lg_transfer INTEGER,
    player_lg_damage_enemy INTEGER,
    player_lg_damage_team INTEGER,
    player_lg_kills_enemy INTEGER,
    player_lg_kills_team INTEGER,
    player_health15_taken INTEGER,
    player_health25_taken INTEGER,
    player_health100_taken INTEGER,
    player_ga_taken INTEGER,
    player_ya_taken INTEGER,
    player_ra_taken INTEGER,
    player_quad_taken INTEGER,
    player_quad_time INTEGER,
    player_pent_taken INTEGER,
    player_ring_taken INTEGER,
    player_ring_time INTEGER,
    PRIMARY KEY(match_id, player_name)
);
```

## Update Normals

Using all matches recorded in the database, compute the regional mean and standard deviation for all statistics of interest. For finite statistical populations such as these, the standard deviation is defined as the square root of the variance, which is the average of the squared deviations from the mean.

For example, if we concatenated the player frag counts from the scoreboards of every 4on4 match played in the European region, we would end up with a population

```math
P=(s_1,s_2,\dots,s_n)
```

consisting of thousands of raw player scores (in this case, frag counts). We would then calculate the arithmetic mean

```math
\mu=\frac{1}{n}\sum_{i=1}^{n}{s_i}
```

and the standard deviation

```math
\sigma=\sqrt{\frac{1}{n}\sum_{i=1}^{n}{(s_i-\mu)^2}}.
```

As stated above, we compute these values for every statistic of interest, then we store the results in tables with the following schemas:

```sql
CREATE TABLE standard_deviations(
    server_region TEXT,
    standard_deviation_frags REAL,
    standard_deviation_frags_minus_deaths REAL,
    standard_deviation_teamkills REAL,
    standard_deviation_efficiency REAL,
    standard_deviation_rl_accuracy REAL,
    standard_deviation_lg_accuracy REAL,
    standard_deviation_gl_accuracy REAL,
    standard_deviation_sg_accuracy REAL,
    standard_deviation_ssg_accuracy REAL,
    standard_deviation_rl_damage_enemy REAL,
    standard_deviation_rl_directs REAL,
    standard_deviation_ga_taken REAL,
    standard_deviation_ya_taken REAL,
    standard_deviation_ra_taken REAL,
    standard_deviation_health100_taken REAL,
    standard_deviation_rl_taken REAL,
    standard_deviation_rl_kills_enemy REAL,
    standard_deviation_rl_dropped REAL,
    standard_deviation_rl_transfer REAL,
    standard_deviation_lg_taken REAL,
    standard_deviation_lg_kills_enemy REAL,
    standard_deviation_lg_damage_enemy REAL,
    standard_deviation_lg_dropped REAL,
    standard_deviation_lg_transfer REAL,
    standard_deviation_damage_taken REAL,
    standard_deviation_damage_given REAL,
    standard_deviation_damage_enemy_weapons REAL,
    standard_deviation_damage_team REAL,
    standard_deviation_damage_self REAL,
    standard_deviation_damage_to_die REAL,
    standard_deviation_quad_taken REAL,
    standard_deviation_pent_taken REAL,
    standard_deviation_ring_taken REAL,
    standard_deviation_spree_frag REAL,
    standard_deviation_spree_quad REAL,
    standard_deviation_spawnfrags REAL,
    standard_deviation_ping REAL,
    PRIMARY KEY(server_region)
);
CREATE TABLE means(
    server_region TEXT,
    mean_frags REAL,
    mean_frags_minus_deaths REAL,
    mean_teamkills REAL,
    mean_efficiency REAL,
    mean_rl_accuracy REAL,
    mean_lg_accuracy REAL,
    mean_gl_accuracy REAL,
    mean_sg_accuracy REAL,
    mean_ssg_accuracy REAL,
    mean_rl_damage_enemy REAL,
    mean_rl_directs REAL,
    mean_ga_taken REAL,
    mean_ya_taken REAL,
    mean_ra_taken REAL,
    mean_health100_taken REAL,
    mean_rl_taken REAL,
    mean_rl_kills_enemy REAL,
    mean_rl_dropped REAL,
    mean_rl_transfer REAL,
    mean_lg_taken REAL,
    mean_lg_kills_enemy REAL,
    mean_lg_damage_enemy REAL,
    mean_lg_dropped REAL,
    mean_lg_transfer REAL,
    mean_damage_taken REAL,
    mean_damage_given REAL,
    mean_damage_enemy_weapons REAL,
    mean_damage_team REAL,
    mean_damage_self REAL,
    mean_damage_to_die REAL,
    mean_quad_taken REAL,
    mean_pent_taken REAL,
    mean_ring_taken REAL,
    mean_spree_frag REAL,
    mean_spree_quad REAL,
    mean_spawnfrags REAL,
    mean_ping REAL,
    PRIMARY KEY(server_region)
);
```

## Update Ratings

Calculate updated ratings for all players in matches occurring since a given cutoff date, which is usually the date of the last recorded match in the database. For each match in the set:

1. Determine the region in which the match took place.

2. Get a list of players participating in the match.

3. Build a list of ratings, one for each participating player, by querying the database. If a player has no existing rating, create one with a correspondingly low confidence component.

4. Compute a match score for each player, as described in the [Methodology](#methodology) section.

5. Sort the player and rating lists by match score descending.

6. Feed the ordered ratings list into TrueSkill, which returns a list of updated ratings in the same order.

7. Write the updated ratings to the database.

The table used to store the ratings has the following schema:

```sql
CREATE TABLE ratings(
    server_region TEXT,
    player_name TEXT,
    rating_mu REAL,
    rating_sigma REAL,
    PRIMARY KEY(server_region, player_name)
);
```

## Export JSON

Export regional rankings to the file `data.json` in the working directory, using the following schema:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Regional Ratings Export",
  "type": "object",
  "required": ["timestamp", "regions"],
  "properties": {
    "timestamp": {
      "type": "string",
      "format": "date-time",
      "description": "ISO-8601 UTC timestamp when the export was generated"
    },
    "regions": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["name", "ratings"],
        "properties": {
          "name": {
            "type": "string",
            "description": "Region name (e.g. Asia, Europe)"
          },
          "ratings": {
            "type": "array",
            "items": {
              "type": "array",
              "minItems": 4,
              "maxItems": 4,
              "items": [
                {
                  "type": "string",
                  "description": "Player name"
                },
                {
                  "type": "integer",
                  "description": "Player rating sigma"
                },
                {
                  "type": "integer",
                  "description": "Player rating mu"
                },
                {
                  "type": "integer",
                  "description": "Number of games played"
                }
              ],
              "description": "One rating entry: [name, sigma, mu, count]"
            }
          }
        },
        "additionalProperties": false
      }
    }
  },
  "additionalProperties": false
}
```

# Usage

The [sync.py](sync.py) script performs zero or more of the previously described operations, specified via switches on the command-line. There is only one positional argument, which is the path to the SQLite database.

```
usage: sync.py [-h] [-s] [-m] [-n] [-r] [-j] [-a DATE] database

positional arguments:
  database          path to database file

options:
  -h, --help        show this help message and exit
  -s, --servers     update the table of servers
  -m, --matches     update the table of matches and players
  -n, --normals     update the table of means and standard deviations
  -r, --ratings     update the table of player ratings
  -j, --json        generate JSON file with player ratings
  -a, --after DATE  set past cutoff date for updates (ISO 8601 format)
```

Python dependencies are listed in the [requirements.txt](requirements.txt) file.

If you wish to run this project locally, you can save yourself time (and bandwidth) by starting with an existing database, accessible [here](https://qw-4on4-ratings.netlify.app/4on4.db.gz). Otherwise you will need to scrape a large number of matches from the hub, which is slow.

# Website

Included in this project is a minimal (naive) website that displays tables of player rankings for the various regions with controls for filtering the output. It uses vanilla Javascript and is entirely contained within the [index.html](index.html) file.

Clients fetch content for the tables from a compressed JSON file stored on the server, which was created at build time using the [Export JSON](#export-json) operation of the [sync.py](sync.py) script.

Currently, the site is hosted by [Netlify](https://www.netlify.com/). It can be accessed at <https://qw-4on4-ratings.netlify.app>.

There is a bash script, [update.sh](update.sh), which performs the following actions:

1. Activate the python virtual environment if present.
2. Update the local database with all unrecorded matches from the hub.
3. Update the player ratings in the local database.
4. Export player ratings from the local database to compressed JSON for publication, accessible on the server at [/data.json.gz](https://qw-4on4-ratings.netlify.app/data.json.gz).
5. Create a compressed copy of the local database for publication, accessible on the server at [/4on4.db.gz](https://qw-4on4-ratings.netlify.app/4on4.db.gz).
6. Build the site for production.
7. Publish the site to the hosting service.

If you plan to build the site, first install the dependencies via `npm install` in the project directory. Of course, you'll need to provide your own hosting solution.
