# dev

- Update to be compatible with networkx 2.0.  There should be no
  changes needed with networkx 1.11 (it should still work), but bugs
  are possible.  Please report.  Github #34

- Allow one to incrementally update databases with new GTFS files:
  `INSERT OR REPLACE` into existing databases (and don't create
  indexes if they already exist).  It is possible that there are
  subtle bugs here - check carefully and report back.  #27, thanks to
  @evelyn9191 for the change.




