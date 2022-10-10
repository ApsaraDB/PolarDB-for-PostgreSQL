#!/usr/bin/awk -f

BEGIN              { inTrace = 0; }

/logTimedTrace$/      { inTrace = 1; next; }
/^2/                  { inTrace = 0; }
                       { if (inTrace == 1) print }
