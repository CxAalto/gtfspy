# This script updates the HSL ajoaika_gps data.  It downloads any new
# files (not touching old ones), then unzips only the ones that aren't
# already there as CSV.

DIR=scratch/

# Before running, you must delete index.html or else it will re-use that old listing.

# This is re-runnable (because of the -c) option and will only get the
# new data

(cd $DIR && wget -r --no-parent --no-host --no-clobber http://dev.hsl.fi/ajoaika_gps/ )

# This can not be re-run without unzipping everything again.  Should
# be changed to be re-runnable efficiently.
#(cd $DIR && ls *.zip  )
(cd $DIR/ajoaika_gps/ && find . -name '*.zip' \( -exec bash -c 'x={}; test -e ${x%%.zip}.csv' \; -o -print -exec unzip {} \; \) )

