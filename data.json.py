#!/usr/bin/env python3

# Command line parsing.
import argparse

# For interacting with the database.
import sqlite3

# Timestamps.
from datetime import datetime, timezone

if __name__ == '__main__':
    # Parse the command line.
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='path to database file')
    parser.add_argument('-w', '--web-date', metavar='DATE', help='date when the website was last updated')
    args = parser.parse_args()

    # Open a connection to the database.
    database = sqlite3.connect(args.database, autocommit=False)

    # Get the current date.
    current_date = datetime.now(timezone.utc).replace(microsecond=0)

    # Get the web date.
    if args.web_date is not None:
        web_date = datetime.fromisoformat(args.web_date).astimezone(timezone.utc).replace(microsecond=0)
    else:
        web_date = current_date

    # Verify that the web date isn't in the future.
    if web_date > current_date:
        raise RuntimeError('Web date is in the future')

    # Convert both dates to ISO 8601 strings.
    current_date = current_date.isoformat()
    web_date = web_date.isoformat()

    # Summarize.
    print(f'Exporting ratings with changes since {web_date}...')

    # Open the data.json file for writing.
    with open('data.json', 'w') as stream:

        # Start the root object.
        stream.write(f'{{"timestamp":"{current_date}","regions":[')

        # Number of regions written.
        region_count = 0

        # Iterate over all the region names.
        for server_region, in database.execute('SELECT DISTINCT server_region from ratings'):

            # Write the trailing comma if necessary.
            if region_count > 0:
                stream.write(',')

            # Start the region.
            stream.write(f'{{"name":"{server_region}","ratings":[')

            # Query the names, ratings, total matches played, and last-played dates for all rated players in the region.
            rows = database.execute(
                '''
                with
                    region_ratings as (
                        select
                            player_name,
                            rating_date,
                            rating_mu,
                            rating_sigma,
                            unixepoch(:web_date) - unixepoch(rating_date) as rating_secs_till_web_date
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
                            row_number() over (partition by player_name order by rating_date desc) as rating_index
                        from
                            region_ratings
                    ),
                    region_ratings_occurring_on_or_before_web_date_indexed_by_secs_till_web_date as (
                        select
                            player_name,
                            rating_mu,
                            rating_sigma,
                            row_number() over (partition by player_name order by rating_secs_till_web_date asc) as rating_index
                        from
                            region_ratings
                        where
                            rating_secs_till_web_date >= 0
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
                            region_ratings_occurring_on_or_before_web_date_indexed_by_secs_till_web_date
                        where
                            rating_index = 1
                    ),
                    region_totals as (
                        select
                            player_name,
                            count() as total_matches_played,
                            date(max(rating_date)) as last_played_date
                        from
                            region_ratings
                        group by
                            player_name
                    )
                select
                    player_name,
                    cast(round(current_rating_mu - 3 * current_rating_sigma) as int) as current_rating,
                    cast(round(prior_rating_mu - 3 * prior_rating_sigma) as int) as prior_rating,
                    total_matches_played,
                    last_played_date
                from
                    region_ratings_current
                        left join region_ratings_prior using (player_name)
                        left join region_totals using (player_name)
                ''',
                {'server_region': server_region, 'web_date': web_date}
            )

            # Number of ratings written.
            rating_count = 0

            # Iterate over all rows returned.
            for player_name, current_rating, prior_rating, total_matches_played, last_played_date in rows:

                # Write the trailing comma if necessary.
                if rating_count > 0:
                    stream.write(',')

                # Write the rating.
                stream.write(f'["{player_name.replace('\\', '\\\\')}",{current_rating},{'null' if prior_rating is None else prior_rating},{total_matches_played},"{last_played_date}"]')
                rating_count += 1

            # Finish the region.
            stream.write(']}')
            region_count += 1

        # Finish the root object.
        stream.write(']}')
