#!/usr/bin/env python3

# For parsing the command line.
import argparse

# For interacting with the database.
import sqlite3

# For making HTTP requests.
import requests

# For talking to QW servers.
import socket, select, time

# For calculating ratings.
import trueskill, collections, re

# Dictionary mapping countries to regions.
REGIONS = {
    'AF': 'Asia',
    'AX': 'Europe',
    'AL': 'Europe',
    'DZ': 'Africa',
    'AS': 'Oceania',
    'AD': 'Europe',
    'AO': 'Africa',
    'AI': 'South America',
    'AG': 'South America',
    'AR': 'South America',
    'AM': 'Asia',
    'AW': 'South America',
    'AU': 'Oceania',
    'AT': 'Europe',
    'AZ': 'Asia',
    'BS': 'South America',
    'BH': 'Asia',
    'BD': 'Asia',
    'BB': 'South America',
    'BY': 'Europe',
    'BE': 'Europe',
    'BZ': 'South America',
    'BJ': 'Africa',
    'BM': 'North America',
    'BT': 'Asia',
    'BO': 'South America',
    'BQ': 'South America',
    'BA': 'Europe',
    'BW': 'Africa',
    'BV': 'South America',
    'BR': 'South America',
    'IO': 'Africa',
    'BN': 'Asia',
    'BG': 'Europe',
    'BF': 'Africa',
    'BI': 'Africa',
    'CV': 'Africa',
    'KH': 'Asia',
    'CM': 'Africa',
    'CA': 'North America',
    'KY': 'South America',
    'CF': 'Africa',
    'TD': 'Africa',
    'CL': 'South America',
    'CN': 'Asia',
    'CX': 'Oceania',
    'CC': 'Oceania',
    'CO': 'South America',
    'KM': 'Africa',
    'CG': 'Africa',
    'CD': 'Africa',
    'CK': 'Oceania',
    'CR': 'South America',
    'CI': 'Africa',
    'HR': 'Europe',
    'CU': 'South America',
    'CW': 'South America',
    'CY': 'Asia',
    'CZ': 'Europe',
    'DK': 'Europe',
    'DJ': 'Africa',
    'DM': 'South America',
    'DO': 'South America',
    'EC': 'South America',
    'EG': 'Africa',
    'SV': 'South America',
    'GQ': 'Africa',
    'ER': 'Africa',
    'EE': 'Europe',
    'SZ': 'Africa',
    'ET': 'Africa',
    'FK': 'South America',
    'FO': 'Europe',
    'FJ': 'Oceania',
    'FI': 'Europe',
    'FR': 'Europe',
    'GF': 'South America',
    'PF': 'Oceania',
    'TF': 'Africa',
    'GA': 'Africa',
    'GM': 'Africa',
    'GE': 'Asia',
    'DE': 'Europe',
    'GH': 'Africa',
    'GI': 'Europe',
    'GR': 'Europe',
    'GL': 'North America',
    'GD': 'South America',
    'GP': 'South America',
    'GU': 'Oceania',
    'GT': 'South America',
    'GG': 'Europe',
    'GN': 'Africa',
    'GW': 'Africa',
    'GY': 'South America',
    'HT': 'South America',
    'HM': 'Oceania',
    'VA': 'Europe',
    'HN': 'South America',
    'HK': 'Asia',
    'HU': 'Europe',
    'IS': 'Europe',
    'IN': 'Asia',
    'ID': 'Asia',
    'IR': 'Asia',
    'IQ': 'Asia',
    'IE': 'Europe',
    'IM': 'Europe',
    'IL': 'Asia',
    'IT': 'Europe',
    'JM': 'South America',
    'JP': 'Asia',
    'JE': 'Europe',
    'JO': 'Asia',
    'KZ': 'Asia',
    'KE': 'Africa',
    'KI': 'Oceania',
    'KP': 'Asia',
    'KR': 'Asia',
    'KW': 'Asia',
    'KG': 'Asia',
    'LA': 'Asia',
    'LV': 'Europe',
    'LB': 'Asia',
    'LS': 'Africa',
    'LR': 'Africa',
    'LY': 'Africa',
    'LI': 'Europe',
    'LT': 'Europe',
    'LU': 'Europe',
    'MO': 'Asia',
    'MG': 'Africa',
    'MW': 'Africa',
    'MY': 'Asia',
    'MV': 'Asia',
    'ML': 'Africa',
    'MT': 'Europe',
    'MH': 'Oceania',
    'MQ': 'South America',
    'MR': 'Africa',
    'MU': 'Africa',
    'YT': 'Africa',
    'MX': 'South America',
    'FM': 'Oceania',
    'MD': 'Europe',
    'MC': 'Europe',
    'MN': 'Asia',
    'ME': 'Europe',
    'MS': 'South America',
    'MA': 'Africa',
    'MZ': 'Africa',
    'MM': 'Asia',
    'NR': 'Oceania',
    'NP': 'Asia',
    'NL': 'Europe',
    'NC': 'Oceania',
    'NZ': 'Oceania',
    'NI': 'South America',
    'NE': 'Africa',
    'NG': 'Africa',
    'NU': 'Oceania',
    'NF': 'Oceania',
    'MK': 'Europe',
    'MP': 'Oceania',
    'NO': 'Europe',
    'OM': 'Asia',
    'PK': 'Asia',
    'PW': 'Oceania',
    'PS': 'Asia',
    'PA': 'South America',
    'PG': 'Oceania',
    'PY': 'South America',
    'PE': 'South America',
    'PH': 'Asia',
    'PN': 'Oceania',
    'PL': 'Europe',
    'PT': 'Europe',
    'PR': 'South America',
    'QA': 'Asia',
    'RE': 'Africa',
    'RO': 'Europe',
    'RU': 'Europe',
    'RW': 'Africa',
    'BL': 'South America',
    'SH': 'Africa',
    'KN': 'South America',
    'LC': 'South America',
    'MF': 'South America',
    'PM': 'North America',
    'VC': 'South America',
    'WS': 'Oceania',
    'SM': 'Europe',
    'ST': 'Africa',
    'SA': 'Asia',
    'SN': 'Africa',
    'RS': 'Europe',
    'SC': 'Africa',
    'SL': 'Africa',
    'SG': 'Asia',
    'SX': 'South America',
    'SK': 'Europe',
    'SI': 'Europe',
    'SB': 'Oceania',
    'SO': 'Africa',
    'ZA': 'Africa',
    'GS': 'South America',
    'SS': 'Africa',
    'ES': 'Europe',
    'LK': 'Asia',
    'SD': 'Africa',
    'SR': 'South America',
    'SJ': 'Europe',
    'SE': 'Europe',
    'CH': 'Europe',
    'SY': 'Asia',
    'TJ': 'Asia',
    'TZ': 'Africa',
    'TH': 'Asia',
    'TL': 'Asia',
    'TG': 'Africa',
    'TK': 'Oceania',
    'TO': 'Oceania',
    'TT': 'South America',
    'TN': 'Africa',
    'TR': 'Asia',
    'TM': 'Asia',
    'TC': 'South America',
    'TV': 'Oceania',
    'UG': 'Africa',
    'UA': 'Europe',
    'AE': 'Asia',
    'GB': 'Europe',
    'US': 'North America',
    'UM': 'Oceania',
    'UY': 'South America',
    'UZ': 'Asia',
    'VU': 'Oceania',
    'VE': 'South America',
    'VN': 'Asia',
    'VG': 'South America',
    'VI': 'South America',
    'WF': 'Oceania',
    'EH': 'Africa',
    'YE': 'Asia',
    'ZM': 'Africa',
    'ZW': 'Africa',
}

# Stats used in player ranking with associated SQL expression.
STATS = {
    'frags': 'player_frags',
    'frags_minus_deaths': 'player_frags - player_deaths',
    'teamkills': 'player_teamkills',
    'efficiency': 'iif(player_frags + player_deaths != 0, player_frags / CAST(player_frags + player_deaths AS REAL), 0)',
    'rl_accuracy': 'iif(player_rl_attacks != 0, CAST(player_rl_virtual AS REAL) / player_rl_attacks, 0)',
    'lg_accuracy': 'iif(player_lg_attacks != 0, CAST(player_lg_hits AS REAL) / player_lg_attacks, 0)',
    'gl_accuracy': 'iif(player_gl_attacks != 0, CAST(player_gl_virtual AS REAL) / player_gl_attacks, 0)',
    'sg_accuracy': 'iif(player_sg_attacks != 0, CAST(player_sg_hits AS REAL) / player_sg_attacks, 0)',
    'ssg_accuracy': 'iif(player_ssg_attacks != 0, CAST(player_ssg_hits AS REAL) / player_ssg_attacks, 0)',
    'rl_damage_enemy': 'player_rl_damage_enemy',
    'rl_directs': 'player_rl_directs',
    'ga_taken': 'player_ga_taken',
    'ya_taken': 'player_ya_taken',
    'ra_taken': 'player_ra_taken',
    'health100_taken': 'player_health100_taken',
    'rl_taken': 'player_rl_taken',
    'rl_kills_enemy': 'player_rl_kills_enemy',
    'rl_dropped': 'player_rl_dropped',
    'rl_transfer': 'player_rl_transfer',
    'lg_taken': 'player_lg_taken',
    'lg_kills_enemy': 'player_lg_kills_enemy',
    'lg_damage_enemy': 'player_lg_damage_enemy',
    'lg_dropped': 'player_lg_dropped',
    'lg_transfer': 'player_lg_transfer',
    'damage_taken': 'player_damage_taken',
    'damage_given': 'player_damage_given',
    'damage_enemy_weapons': 'player_damage_enemy_weapons',
    'damage_team': 'player_damage_team',
    'damage_self': 'player_damage_self',
    'damage_to_die': 'player_damage_to_die',
    'quad_taken': 'player_quad_taken',
    'pent_taken': 'player_pent_taken',
    'ring_taken': 'player_ring_taken',
    'spree_frag': 'player_spree_frag',
    'spree_quad': 'player_spree_quad',
    'spawnfrags': 'player_spawnfrags',
    'ping': 'player_ping',
}

# Subset of columns from the "players" table needed to calculate a score.
SCORE_COLUMNS = set(match.group(0) for match in re.finditer(r'player_\w+', ','.join(STATS.values()) + ',player_name'))

# Named tuple representation of the subset of columns from the "players" table needed to calculate a score.
Player = collections.namedtuple('Player', ' '.join(column[7:] for column in SCORE_COLUMNS))

# Escape all colors and unprintable characters in a QW string.
def escape(s):
    def ord2chr(o):
        if o == 0x10:
            return '\\1['
        elif o == 0x11:
            return '\\1]'
        elif o >= 0x12 and o <= 0x1b:
            return f'\\2{chr(o + 30)}'
        elif o >= 0x20 and o <= 0x7e:
            if o == 0x5c:
                return '\\\\'
            else:
                return chr(o)
        elif o == 0x90:
            return '\\3['
        elif o == 0x91:
            return '\\3]'
        elif o >= 0x92 and o <= 0x9b:
            return f'\\4{chr(o - 98)}'
        elif o >= 0xa0 and o <= 0xfe:
            if o == 0xdc:
                return '\\5\\\\'
            else:
                return f'\\5{chr(o - 128)}'
        else:
            return f'\\x{o:02x}'
    if type(s) is str:
        return ''.join(ord2chr(ord(c)) for c in s)
    if type(s) in (bytes, bytearray):
        return ''.join(ord2chr(o) for o in s)
    raise TypeError(f'Cannot escape value of type {type(s)}')

# Extract a value (safely) from a dictionary of dictionaries.
def dig(root, *path, **kwargs):
    for component in path:
        try:
            root = root[component]
        except KeyError as error:
            try:
                return kwargs['default']
            except KeyError:
                raise error
    return root

# Ask a QW server for its hostname.
def hostname(ip, port, timeout=5):
    dest = ip, port
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(b'\xff\xff\xff\xffstatus 23\n', dest)
    start = time.monotonic()
    while True:
        rlist, _, _ = select.select([sock], [], [], timeout)
        if len(rlist) == 0:
            raise TimeoutError()
        data, origin = sock.recvfrom(65507) # max UDP payload size
        if origin != dest:
            elapsed = time.monotonic() - start
            timeout = max(0, timeout - elapsed)
            continue
        i = data.find(b'hostname\\') + 9
        if i < 9:
            raise KeyError()
        j = data.find(b'\\', i)
        if j < 0:
            j = data.find(b'\n', i)
            if j < 0:
                raise ValueError()
        return escape(data[i:j])

# Update the "servers" table.
def servers(database):
    # Create the table if necessary.
    cursor = database.cursor()
    cursor.executescript('CREATE TABLE IF NOT EXISTS servers(server_name TEXT, server_region TEXT, PRIMARY KEY(server_name));')

    # Fetch a list of servers from the internet.
    response = requests.get('https://www.quakeservers.net/lists/servers/global.txt', stream=True)
    response.raise_for_status()

    # Start a cache of addresses.
    cache = {}

    # Initialize processed counter.
    count = 0

    for line in response.iter_lines(decode_unicode=True):
        # Increment the processed counter.
        count += 1

        # Parse out the address and port.
        ip, port = line.split(':')
        port = int(port)
        print(f'{ip}:{port} → ', end='', flush=True)

        # Ask the server for its hostname.
        try:
            server_name = hostname(ip, port)
        except TimeoutError:
            print('timed out')
            continue
        except KeyError:
            print('missing hostname key')
            continue
        except ValueError:
            print('missing hostname value')
            continue
        print(f'{server_name} → ', end='', flush=True)

        # Check if the server already exists in the database.
        row = cursor.execute('SELECT server_region FROM servers WHERE server_name=?', (server_name,)).fetchone()
        if row:
            print(row[0])
            continue

        # Map the address to a region.
        server_region = cache.get(ip)
        if server_region is None:
            try:
                response = requests.get(f'https://ipinfo.io/{ip}')
                response.raise_for_status()
                json = response.json()
            except requests.RequestException as error:
                print(error)
                continue
            country = json.get('country')
            if country is None:
                print('missing country')
                continue
            server_region = REGIONS.get(country)
            if server_region is None:
                print('unknown region')
                continue
            cache[ip] = server_region
        print(server_region)

        # Insert a row into the table.
        cursor.execute('INSERT INTO servers(server_name, server_region) VALUES (?,?)', (server_name, server_region))

    # Print the processed count.
    print(f'Processed {count} servers')

# Update the "matches" and "players" tables.
def matches(database, after):
    # Create the tables if necessary.
    cursor = database.cursor()
    cursor.executescript(
        '''
        CREATE TABLE IF NOT EXISTS matches(
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
        CREATE TABLE IF NOT EXISTS players(
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
        '''
    )

    # Authorization headers used in requests to the hub. These are not supposed to be secret.
    auth_headers = {
        'apikey': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5jc3Boa2pmb21pbmlteHp0amlwIiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTY5Mzg1NjMsImV4cCI6MjAxMjUxNDU2M30.NN6hjlEW-qB4Og9hWAVlgvUdwrbBO13s8OkAJuBGVbo',
        'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5jc3Boa2pmb21pbmlteHp0amlwIiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTY5Mzg1NjMsImV4cCI6MjAxMjUxNDU2M30.NN6hjlEW-qB4Og9hWAVlgvUdwrbBO13s8OkAJuBGVbo',
    }

    # URL for fetching basic match information.
    info_url = 'https://ncsphkjfominimxztjip.supabase.co/rest/v1/v1_games'

    # Fetch the number of remote matches available.
    response = requests.get(info_url, params={'select': 'count', 'mode': 'eq.4on4', 'timestamp': f'gt.{after}'}, headers=auth_headers)
    response.raise_for_status()
    remote_match_count = response.json()[0]['count']

    # Process the remote matches in batches.
    processed_match_count = 0
    while processed_match_count < remote_match_count:
        # Fetch the next batch of remote matches.
        response = requests.get(info_url, params={'select': 'id,timestamp,demo_sha256', 'mode': 'eq.4on4', 'timestamp': f'gt.{after}', 'offset': processed_match_count, 'limit': 1000}, headers=auth_headers)
        response.raise_for_status()
        matches = response.json()

        # Update the processed counter.
        processed_match_count += len(matches)

        for match in matches:
            # Skip the match if it already exists in the database.
            match_id = match['id']
            print(f'{match_id} → ', end='', flush=True)
            cursor.execute('SELECT EXISTS(SELECT * FROM matches WHERE match_id=?)', (match_id,))
            if cursor.fetchone()[0]:
                print('ok')
                continue

            # Fetch the KTX stats for the match.
            match_demo_sha256 = match['demo_sha256']
            try:
                response = requests.get(f'https://d.quake.world/{match_demo_sha256[:3]}/{match_demo_sha256}.mvd.ktxstats.json')
                response.raise_for_status()
                ktx = response.json()
            except requests.RequestException as error:
                print(error)
                continue

            # When players drop and subsequently rejoin, they can end up appearing multiple times in the stats record.
            # We need to remove duplicates before updating the database, otherwise constraints on uniqueness could be
            # violated.
            distinct_names = set()
            distinct_players = []
            for player in ktx['players']:
                name = player['name']
                if name not in distinct_names:
                    distinct_names.add(name)
                    distinct_players.append(player)

            # Update the matches table.
            match_date = match['timestamp']
            match_tag = ktx.get('matchtag')
            match_map = ktx['map']
            server_name = escape(ktx['hostname'])
            server_port = ktx['port']
            match_deathmatch_mode = ktx['dm']
            match_teamplay_mode = ktx['tp']
            match_time_limit_mins = ktx['tl']
            match_duration_secs = ktx['duration']
            cursor.execute(
                '''
                INSERT INTO matches(
                    match_id,
                    match_date,
                    match_tag,
                    match_map,
                    server_name,
                    server_port,
                    match_deathmatch_mode,
                    match_teamplay_mode,
                    match_time_limit_mins,
                    match_duration_secs,
                    match_demo_sha256
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ''',
                (
                    match_id,
                    match_date,
                    match_tag,
                    match_map,
                    server_name,
                    server_port,
                    match_deathmatch_mode,
                    match_teamplay_mode,
                    match_time_limit_mins,
                    match_duration_secs,
                    match_demo_sha256
                )
            )

            # Update the players table.
            for player in distinct_players:
                player_name = escape(player['name'])
                player_login = player['login']
                player_team = escape(player['team'])
                player_top_color = dig(player, 'top-color')
                player_bottom_color = dig(player, 'bottom-color')
                player_ping = player['ping']
                player_frags = dig(player, 'stats', 'frags')
                player_deaths = dig(player, 'stats', 'deaths')
                player_teamkills = dig(player, 'stats', 'tk')
                player_spawnfrags = dig(player, 'stats', 'spawn-frags')
                player_suicides = dig(player, 'stats', 'suicides')
                player_damage_taken = dig(player, 'dmg', 'taken')
                player_damage_given = dig(player, 'dmg', 'given')
                player_damage_team = dig(player, 'dmg', 'team')
                player_damage_self = dig(player, 'dmg', 'self')
                player_damage_team_weapons = dig(player, 'dmg', 'team-weapons')
                player_damage_enemy_weapons = dig(player, 'dmg', 'enemy-weapons')
                player_damage_to_die = dig(player, 'dmg', 'taken-to-die')
                player_spree_frag = dig(player, 'spree', 'max')
                player_spree_quad = dig(player, 'spree', 'quad')
                player_speed_max = dig(player, 'speed', 'max')
                player_speed_avg = dig(player, 'speed', 'avg')
                player_sg_attacks = dig(player, 'weapons', 'sg', 'acc', 'attacks', default=0)
                player_sg_hits = dig(player, 'weapons', 'sg', 'acc', 'hits', default=0)
                player_sg_damage_enemy = dig(player, 'weapons', 'sg', 'damage', 'enemy', default=0)
                player_sg_damage_team = dig(player, 'weapons', 'sg', 'damage', 'team', default=0)
                player_ssg_attacks = dig(player, 'weapons', 'ssg', 'acc', 'attacks', default=0)
                player_ssg_hits = dig(player, 'weapons', 'ssg', 'acc', 'hits', default=0)
                player_ssg_damage_enemy = dig(player, 'weapons', 'ssg', 'damage', 'enemy', default=0)
                player_ssg_damage_team = dig(player, 'weapons', 'ssg', 'damage', 'team', default=0)
                player_gl_attacks = dig(player, 'weapons', 'gl', 'acc', 'attacks', default=0)
                player_gl_directs = dig(player, 'weapons', 'gl', 'acc', 'hits', default=0)
                player_gl_virtual = dig(player, 'weapons', 'gl', 'acc', 'virtual', default=0)
                player_rl_attacks = dig(player, 'weapons', 'rl', 'acc', 'attacks', default=0)
                player_rl_directs = dig(player, 'weapons', 'rl', 'acc', 'hits', default=0)
                player_rl_virtual = dig(player, 'weapons', 'rl', 'acc', 'virtual', default=0)
                player_rl_dropped = dig(player, 'weapons', 'rl', 'pickups', 'dropped', default=0)
                player_rl_taken = dig(player, 'weapons', 'rl', 'pickups', 'taken', default=0)
                player_rl_transfer = dig(player, 'xferRL', default=0)
                player_rl_damage_enemy = dig(player, 'weapons', 'rl', 'damage', 'enemy', default=0)
                player_rl_damage_team = dig(player, 'weapons', 'rl', 'damage', 'team', default=0)
                player_rl_kills_enemy = dig(player, 'weapons', 'rl', 'kills', 'enemy', default=0)
                player_rl_kills_team = dig(player, 'weapons', 'rl', 'kills', 'team', default=0)
                player_lg_attacks = dig(player, 'weapons', 'lg', 'acc', 'attacks', default=0)
                player_lg_hits = dig(player, 'weapons', 'lg', 'acc', 'hits', default=0)
                player_lg_dropped = dig(player, 'weapons', 'lg', 'pickups', 'dropped', default=0)
                player_lg_taken = dig(player, 'weapons', 'lg', 'pickups', 'taken', default=0)
                player_lg_transfer = dig(player, 'xferLG', default=0)
                player_lg_damage_enemy = dig(player, 'weapons', 'lg', 'damage', 'enemy', default=0)
                player_lg_damage_team = dig(player, 'weapons', 'lg', 'damage', 'team', default=0)
                player_lg_kills_enemy = dig(player, 'weapons', 'lg', 'kills', 'enemy', default=0)
                player_lg_kills_team = dig(player, 'weapons', 'lg', 'kills', 'team', default=0)
                player_health15_taken = dig(player, 'items', 'health_15', 'took', default=0)
                player_health25_taken = dig(player, 'items', 'health_25', 'took', default=0)
                player_health100_taken = dig(player, 'items', 'health_100', 'took', default=0)
                player_ga_taken = dig(player, 'items', 'ga', 'took', default=0)
                player_ya_taken = dig(player, 'items', 'ya', 'took', default=0)
                player_ra_taken = dig(player, 'items', 'ra', 'took', default=0)
                player_quad_taken = dig(player, 'items', 'q', 'took', default=0)
                player_quad_time = dig(player, 'items', 'q', 'time', default=0)
                player_pent_taken = dig(player, 'items', 'p', 'took', default=0)
                player_ring_taken = dig(player, 'items', 'r', 'took', default=0)
                player_ring_time = dig(player, 'items', 'r', 'time', default=0)
                cursor.execute(
                    '''
                    INSERT INTO players(
                        match_id,
                        player_name,
                        player_login,
                        player_team,
                        player_top_color,
                        player_bottom_color,
                        player_ping,
                        player_frags,
                        player_deaths,
                        player_teamkills,
                        player_spawnfrags,
                        player_suicides,
                        player_damage_taken,
                        player_damage_given,
                        player_damage_team,
                        player_damage_self,
                        player_damage_team_weapons,
                        player_damage_enemy_weapons,
                        player_damage_to_die,
                        player_spree_frag,
                        player_spree_quad,
                        player_speed_max,
                        player_speed_avg,
                        player_sg_attacks,
                        player_sg_hits,
                        player_sg_damage_enemy,
                        player_sg_damage_team,
                        player_ssg_attacks,
                        player_ssg_hits,
                        player_ssg_damage_enemy,
                        player_ssg_damage_team,
                        player_gl_attacks,
                        player_gl_directs,
                        player_gl_virtual,
                        player_rl_attacks,
                        player_rl_directs,
                        player_rl_virtual,
                        player_rl_dropped,
                        player_rl_taken,
                        player_rl_transfer,
                        player_rl_damage_enemy,
                        player_rl_damage_team,
                        player_rl_kills_enemy,
                        player_rl_kills_team,
                        player_lg_attacks,
                        player_lg_hits,
                        player_lg_dropped,
                        player_lg_taken,
                        player_lg_transfer,
                        player_lg_damage_enemy,
                        player_lg_damage_team,
                        player_lg_kills_enemy,
                        player_lg_kills_team,
                        player_health15_taken,
                        player_health25_taken,
                        player_health100_taken,
                        player_ga_taken,
                        player_ya_taken,
                        player_ra_taken,
                        player_quad_taken,
                        player_quad_time,
                        player_pent_taken,
                        player_ring_taken,
                        player_ring_time
                    )
                    VALUES(
                        ?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?
                    )
                    ''',
                    (
                        match_id,
                        player_name,
                        player_login,
                        player_team,
                        player_top_color,
                        player_bottom_color,
                        player_ping,
                        player_frags,
                        player_deaths,
                        player_teamkills,
                        player_spawnfrags,
                        player_suicides,
                        player_damage_taken,
                        player_damage_given,
                        player_damage_team,
                        player_damage_self,
                        player_damage_team_weapons,
                        player_damage_enemy_weapons,
                        player_damage_to_die,
                        player_spree_frag,
                        player_spree_quad,
                        player_speed_max,
                        player_speed_avg,
                        player_sg_attacks,
                        player_sg_hits,
                        player_sg_damage_enemy,
                        player_sg_damage_team,
                        player_ssg_attacks,
                        player_ssg_hits,
                        player_ssg_damage_enemy,
                        player_ssg_damage_team,
                        player_gl_attacks,
                        player_gl_directs,
                        player_gl_virtual,
                        player_rl_attacks,
                        player_rl_directs,
                        player_rl_virtual,
                        player_rl_dropped,
                        player_rl_taken,
                        player_rl_transfer,
                        player_rl_damage_enemy,
                        player_rl_damage_team,
                        player_rl_kills_enemy,
                        player_rl_kills_team,
                        player_lg_attacks,
                        player_lg_hits,
                        player_lg_dropped,
                        player_lg_taken,
                        player_lg_transfer,
                        player_lg_damage_enemy,
                        player_lg_damage_team,
                        player_lg_kills_enemy,
                        player_lg_kills_team,
                        player_health15_taken,
                        player_health25_taken,
                        player_health100_taken,
                        player_ga_taken,
                        player_ya_taken,
                        player_ra_taken,
                        player_quad_taken,
                        player_quad_time,
                        player_pent_taken,
                        player_ring_taken,
                        player_ring_time
                    )
                )

            print('ok')

    # Print the number of matches processed.
    print(f'Processed {processed_match_count} matches')

# Update the "means" and "standard_deviations" tables.
def normals(database):
    # Create the tables if necessary.
    database.executescript('CREATE TABLE IF NOT EXISTS means(server_region TEXT,' + ','.join(f'mean_{key} REAL' for key in STATS.keys()) + ',PRIMARY KEY(server_region));')
    database.executescript('CREATE TABLE IF NOT EXISTS standard_deviations(server_region TEXT,' + ','.join(f'standard_deviation_{key} REAL' for key in STATS.keys()) + ',PRIMARY KEY(server_region));')

    # Populate the tables.
    database.executescript('INSERT OR REPLACE INTO means SELECT server_region,' + ','.join(f'avg({value})' for value in STATS.values()) + ' FROM servers NATURAL JOIN matches NATURAL JOIN players GROUP BY server_region;')
    database.executescript('INSERT OR REPLACE INTO standard_deviations SELECT server_region,' + ','.join(f'sqrt(avg(pow(({value})-mean_{key},2)))' for key,value in STATS.items()) + ' FROM servers NATURAL JOIN matches NATURAL JOIN players NATURAL JOIN means GROUP BY server_region;')

# Create a closure to compute player scores in a given region.
def scorer(database, server_region):
    # Compute the standard score for a given stat.
    def zscore(value, name):
        mean, stddev = database.execute(f'SELECT mean_{name}, standard_deviation_{name} FROM means NATURAL JOIN standard_deviations WHERE server_region=?', (server_region,)).fetchone()
        return 0 if stddev == 0 else (value - mean) / stddev

    # Compute the match score for a given player.
    def pscore(player):
        # Start with zero.
        score = 0

        # How important is it to get frags?
        score = score + zscore(player.frags, 'frags') * 7.313

        # How important is it to have more frags than deaths?
        score = score + zscore(player.frags - player.deaths, 'frags_minus_deaths') * 7.938

        # How important is it to minimize teamkills?
        score = score - zscore(player.teamkills, 'teamkills') * 4.813

        # How important is it to maximize player efficiency?
        if player.frags + player.deaths != 0:
            score = score + zscore(player.frags / (player.frags + player.deaths), 'efficiency') * 6.867

        # How important is it to maximize RL accuracy?
        if player.rl_attacks != 0:
            score = score + zscore(player.rl_virtual / player.rl_attacks, 'rl_accuracy') * 5.125

        # How important is it to maximize LG accuracy?
        if player.lg_attacks != 0:
            score = score + zscore(player.lg_hits / player.lg_attacks, 'lg_accuracy') * 6.267

        # How important is it to maximize GL accuracy?
        if player.gl_attacks != 0:
            score = score + zscore(player.gl_virtual / player.gl_attacks, 'gl_accuracy') * 2.688

        # How important is it to maximize SG accuracy?
        if player.sg_attacks != 0:
            score = score + zscore(player.sg_hits / player.sg_attacks, 'sg_accuracy') * 6.5

        # How important is it to maximize SSG accuracy?
        if player.ssg_attacks != 0:
            score = score + zscore(player.ssg_hits / player.ssg_attacks, 'ssg_accuracy') * 4.938

        # How important is it to maximize RL damage?
        score = score + zscore(player.rl_damage_enemy, 'rl_damage_enemy') * 5.625

        # How important is it to maximize LG damage?
        score = score + zscore(player.lg_damage_enemy, 'lg_damage_enemy') * 5.8

        # How important is it to maximize direct RL hits?
        score = score + zscore(player.rl_directs, 'rl_directs') * 3.188

        # How important is it to collect green armors?
        score = score + zscore(player.ga_taken, 'ga_taken') * 4.625

        # How important is it to collect yellow armors?
        score = score + zscore(player.ya_taken, 'ya_taken') * 6.813

        # How important is it to collect red armors?
        score = score + zscore(player.ra_taken, 'ra_taken') * 8.875

        # How important is it to collect megas?
        score = score + zscore(player.health100_taken, 'health100_taken') * 6.438

        # How important is it to take fresh RLs?
        score = score + zscore(player.rl_taken, 'rl_taken') * 7.438

        # How important is it to kill enemy RLs?
        score = score + zscore(player.rl_kills_enemy, 'rl_kills_enemy') * 8.875

        # How important is it to minimize the number of RLs dropped (not transferred)?
        score = score - zscore(player.rl_dropped, 'rl_dropped') * 7.75

        # How important is it to maximize the number of RLs transferred (after dropping)?
        score = score + zscore(player.rl_transfer, 'rl_transfer') * 6.5

        # How important is it to take fresh LGs?
        score = score + zscore(player.lg_taken, 'lg_taken') * 7.125

        # How important is it to kill enemy LGs?
        score = score + zscore(player.lg_kills_enemy, 'lg_kills_enemy') * 7.813

        # How important is it to minimize the number of LGs dropped?
        score = score - zscore(player.lg_dropped, 'lg_dropped') * 7

        # How important is it to maximize the number of LGs transferred (after dropping)?
        score = score + zscore(player.lg_transfer, 'lg_transfer') * 5.688

        # How important is it to minimze damage taken?
        score = score - zscore(player.damage_taken, 'damage_taken') * 4.875

        # How important is it to maximize damage given?
        score = score + zscore(player.damage_given, 'damage_given') * 7.938

        # How important is it to maximize EWEP?
        score = score + zscore(player.damage_enemy_weapons, 'damage_enemy_weapons') * 7.5

        # How important is it to minimze team damage?
        score = score - zscore(player.damage_team, 'damage_team') * 4.875

        # How important is it to minimize self damage?
        score = score - zscore(player.damage_self, 'damage_self') * 3.625

        # How important is it to maximize ToDie?
        score = score + zscore(player.damage_to_die, 'damage_to_die') * 6.125

        # How important is it to take quads?
        score = score + zscore(player.quad_taken, 'quad_taken') * 8.438

        # How important is it to take pents?
        score = score + zscore(player.pent_taken, 'pent_taken') * 8.75

        # How important are long frag streaks?
        score = score + zscore(player.spree_frag, 'spree_frag') * 4.625

        # How important are quad runs with many kills?
        score = score + zscore(player.spree_quad, 'spree_quad') * 4.688

        # How important is it to get spawn frags?
        score = score + zscore(player.spawnfrags, 'spawnfrags') * 5.625

        # How important is it to take rings?
        score = score + zscore(player.ring_taken, 'ring_taken') * 5.6

        # How important is it to have low ping?
        score = score + zscore(player.ping, 'ping') * 6.8

        # Final result.
        return score

    return pscore

# Update the "ratings" table.
def ratings(database, after):
    # Create a rating environment.
    environment = trueskill.TrueSkill(mu=1500, sigma=500, beta=250, tau=5, draw_probability=0)

    # Create the table if necessary.
    database.executescript('CREATE TABLE IF NOT EXISTS ratings(server_region TEXT, player_name TEXT, rating_mu REAL, rating_sigma REAL, PRIMARY KEY(server_region, player_name));')

    for match_id, server_region in database.execute('SELECT match_id, server_region FROM matches NATURAL JOIN servers WHERE match_date > ? ORDER BY match_date ASC', (after,)):
        # Get a list of players.
        players = map(Player._make, database.execute(f'SELECT {','.join(SCORE_COLUMNS)} FROM players WHERE match_id=?', (match_id,)))

        # Sort the players by score.
        players = sorted(players, key=scorer(database, server_region), reverse=True)

        # Build a list of rating groups.
        rating_groups = []
        for player in players:
            row = database.execute('SELECT rating_mu, rating_sigma FROM ratings WHERE server_region=? AND player_name=?', (server_region, player.name)).fetchone()
            if row is None:
                rating = environment.create_rating()
            else:
                rating = environment.create_rating(row[0], row[1])
            rating_groups.append((rating,))

        # Update the ratings.
        rating_groups = environment.rate(rating_groups)
        for group, player in zip(rating_groups, players):
            rating = group[0]
            database.execute('INSERT OR REPLACE INTO ratings(server_region, player_name, rating_mu, rating_sigma) VALUES(?,?,?,?)', (server_region, player.name, rating.mu, rating.sigma))

# Generate a data.json for the website.
def json(database):
    with open('data.json', 'w') as stream:
        stream.write(f'{{"timestamp":"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}","regions":[')
        region_count, = database.execute('SELECT count(*) FROM (SELECT DISTINCT server_region FROM ratings)').fetchone()
        for region, in database.execute('SELECT DISTINCT server_region from ratings'):
            region_ratings_count, = database.execute('SELECT count(*) FROM ratings WHERE server_region=?', (region,)).fetchone()
            stream.write(f'{{"name":"{region}","ratings":[')
            for player, rating, plus_minus, matches in database.execute('SELECT player_name, rating_mu, rating_sigma, count(*) FROM players NATURAL JOIN matches NATURAL JOIN ratings NATURAL JOIN servers WHERE server_region=? GROUP BY player_name', (region,)):
                stream.write(f'["{player.replace('\\', '\\\\')}",{round(rating)},{round(plus_minus)},{matches}]')
                region_ratings_count -= 1
                if region_ratings_count > 0:
                    stream.write(',')
            stream.write(']}')
            region_count -= 1
            if region_count > 0:
                stream.write(',')
        stream.write(']}')

if __name__ == '__main__':
    # Parse the command line.
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='path to database file')
    parser.add_argument('-s', '--servers', action='store_true', help='update the table of servers')
    parser.add_argument('-m', '--matches', action='store_true', help='update the table of matches and players')
    parser.add_argument('-n', '--normals', action='store_true', help='update the table of means and standard deviations')
    parser.add_argument('-r', '--ratings', action='store_true', help='update the table of player ratings')
    parser.add_argument('-j', '--json', action='store_true', help='generate JSON file with player ratings')
    parser.add_argument('-a', '--after', metavar='DATE', help='set past cutoff date for updates (ISO 8601 format)')
    args = parser.parse_args()

    # Open a connection to the database.
    database = sqlite3.connect(args.database, autocommit=False)

    # Determine the past cutoff for updates.
    after = args.after
    if after is None:
        row = database.execute('SELECT max(match_date) FROM matches').fetchone()
        after = '1970-01-01' if row is None else row[0]

    # Update the servers.
    if args.servers:
        print('Updating servers...')
        servers(database)

    # Update the matches.
    if args.matches:
        print('Updating matches...')
        matches(database, after)

    # Update the normals.
    if args.normals:
        print('Updating normals...')
        normals(database)

    # Update the ratings.
    if args.ratings:
        print('Updating ratings...')
        ratings(database, after)

    # Update data.json.
    if args.json:
        print('Updating data.json')
        json(database)

    # Commit changes to the database.
    database.commit()
