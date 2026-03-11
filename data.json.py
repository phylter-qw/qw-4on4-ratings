#!/usr/bin/env python3

# Command line parsing.
import argparse

# For interacting with the database.
import sqlite3

# Timestamps.
import time

if __name__ == '__main__':
    # Parse the command line.
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='path to database file')
    parser.add_argument('-p', '--prior', metavar='DATE', help='prior update time (ISO 8601 format)')
    args = parser.parse_args()

    # Open a connection to the database.
    database = sqlite3.connect(args.database, autocommit=False)

    # Get a timestamp for the current time UTC.
    current_date = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    # Determine the prior update time.
    prior_date = args.prior
    if prior_date is None:
        prior_date = current_date

    # Summarize.
    print(f'Exporting ratings with changes since {prior_date}...')

    # Open the data.json file for writing.
    with open('data.json', 'w') as stream:

        # Start the root object.
        stream.write(f'{{"timestamp":"{current_date}","regions":[')

        # Pre-determine the number of regions which will be written.
        region_count, = database.execute('SELECT count(*) FROM (SELECT DISTINCT server_region FROM ratings)').fetchone()

        # Iterate over all the region names.
        for server_region, in database.execute('SELECT DISTINCT server_region from ratings'):

            # Pre-determine the number of ratings in this region.
            ratings_count, = database.execute('SELECT count(*) FROM (SELECT DISTINCT player_name FROM ratings WHERE server_region=?)', (server_region,)).fetchone()

            # Start the region object.
            stream.write(f'{{"name":"{server_region}","ratings":[')

            # Query the names, ratings, total matches played, and last-played dates for all players in the region.
            rows = database.execute(
                '''
                with
                    region_ratings as (
                        select
                            player_name,
                            rating_date,
                            rating_mu,
                            rating_sigma,
                            unixepoch(:prior_date) - unixepoch(rating_date) as rating_date_delta
                        from
                            ratings
                        where
                            server_region = :server_region
                    ),
                    region_ratings_indexed_by_date as (
                        select
                            player_name,
                            rating_mu,
                            rating_sigma,
                            row_number() over (
                                partition by player_name
                                order by rating_date desc
                            ) as rating_index
                        from
                            region_ratings
                    ),
                    region_ratings_indexed_by_date_delta as (
                        select
                            player_name,
                            rating_mu,
                            rating_sigma,
                            row_number() over (
                                partition by player_name
                                order by rating_date_delta asc
                            ) as rating_index
                        from
                            region_ratings
                        where
                            rating_date_delta >= 0
                    ),
                    region_ratings_current as (
                        select
                            player_name,
                            rating_mu as current_rating_mu,
                            rating_sigma as current_rating_sigma
                        from
                            region_ratings_indexed_by_date
                        where
                            rating_index = 1
                    ),
                    region_ratings_prior as (
                        select
                            player_name,
                            rating_mu as prior_rating_mu,
                            rating_sigma as prior_rating_sigma
                        from
                            region_ratings_indexed_by_date_delta
                        where
                            rating_index = 1
                    ),
                    region_matches as (
                        select
                            player_name,
                            count(*) as total_matches_played,
                            max(match_date) as last_played_date
                        from
                            matches
                                natural join
                            players
                                natural join
                            servers
                        where
                            server_region = :server_region
                        group by
                            player_name
                    )
                select
                    player_name,
                    cast(round(current_rating_mu - 3*current_rating_sigma) as integer) as current_rating,
                    cast(round(prior_rating_mu - 3*prior_rating_sigma) as integer) prior_rating,
                    total_matches_played,
                    last_played_date
                from
                    region_ratings_current
                        natural join
                    region_ratings_prior
                        natural join
                    region_matches
                ''',
                {'server_region': server_region, 'prior_date': prior_date}
            )

            # Iterate over all rows returned.
            for player_name, current_rating, prior_rating, total_matches_played, last_played_date in rows:

                # Write the player tuple.
                stream.write(f'["{player_name.replace('\\', '\\\\')}",{current_rating},{prior_rating},{total_matches_played},"{last_played_date}"]')
                ratings_count -= 1
                if ratings_count > 0:
                    stream.write(',')

            # Finish the region.
            stream.write(']}')
            region_count -= 1
            if region_count > 0:
                stream.write(',')

        # Finish the root object.
        stream.write(']}')
