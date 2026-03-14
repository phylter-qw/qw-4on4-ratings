#!/usr/bin/env bash

# Exit on error.
set -e

# Load NVM if present.
if [ -z "$NVM_DIR" ]; then
	export NVM_DIR="$HOME/.nvm"
	[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
	[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion
fi

# Load python virtual environment if present.
[ -f pyvenv.cfg ] && \. bin/activate

# Update matches, ratings.
python sync.py -mr 4on4.db

# Generate a data.json.gz file.
if [ -f dist/data.json.gz ]; then
	python data.json.py -w `gzip -dkc dist/data.json.gz | jq -r .timestamp` 4on4.db
else
	python data.json.py 4on4.db
fi
gzip data.json

# Compressed copy of database.
gzip -k 4on4.db

# Move resources to public directory.
mv data.json.gz 4on4.db.gz public

# Deploy site.
if [ "$1" = "deploy" ]; then
	npm run build
	npx ntl deploy --prod --dir dist
fi
