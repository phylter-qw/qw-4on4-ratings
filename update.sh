#!/usr/bin/env bash

# Load NVM if present.
if [ -z "$NVM_DIR" ]; then
	export NVM_DIR="$HOME/.nvm"
	[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
	[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion
fi

# Load python virtual environment if present.
[ -f pyvenv.cfg ] && \. bin/activate

# Update matches, ratings, JSON.
python sync.py -mrj 4on4.db

# Compress existing JSON file.
gzip data.json

# Compressed copy of database.
gzip -k 4on4.db

# Move resources to public directory.
mv data.json.gz 4on4.db.gz public

# Build site.
npm run build

# Deploy site.
npx ntl deploy --prod --dir dist
