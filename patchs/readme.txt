How to use patch:
1. Download PG11.2 source code.
2. Apply HLC patch. Command:  git apply /path/to/patch/xxx.patch
3. Copy contrib/polarx to PG11.2's contrib directory.
4. Compile. Use ./build.sh to compile.
5. Done. Now you can use distributed database with distributed transaction support.
