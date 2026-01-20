#!/bin/sh

[[ -f pyvenv.cfg ]] && . bin/activate
python sync.py -mrj 4on4.db
gzip data.json
gzip -k 4on4.db
mv data.json.gz 4on4.db.gz public
npm run build
npx ntl deploy --prod --dir dist
