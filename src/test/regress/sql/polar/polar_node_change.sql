-- Obtain the generated copyfuncs, equalfuncs, outfuncs, and readfuncs.
-- If changes have occurred, the modifier needs to verify them.
-- If they meet expectations, the results can be directly overwritten.


-- check copyfuncs
\! awk 'BEGIN { skip=1 } /^#include/ { skip=0; next } !skip' ../../backend/nodes/copyfuncs.funcs.c

-- check equalfuncs
\! awk 'BEGIN { skip=1 } /^#include/ { skip=0; next } !skip' ../../backend/nodes/equalfuncs.funcs.c

-- check outfuncs
\! awk 'BEGIN { skip=1 } /^#include/ { skip=0; next } !skip' ../../backend/nodes/outfuncs.funcs.c

-- check readfuncs
\! awk 'BEGIN { skip=1 } /^#include/ { skip=0; next } !skip' ../../backend/nodes/readfuncs.funcs.c
