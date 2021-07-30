#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define MAX_OPTION_VALUE 300
#define MAX_FILE_PATH 600
#define PG_CONTROL_PATH "global"
#define PG_CONTROL_FILE "pg_control"
#define PG_BLOCKLIST_FILE "restore_block_list"
#define READ_BUF 4096

const char *progname;

enum opt_name
{
    datadir = 0
}opt_name;

/* 
    Struct From PolarDB. Used to function read_block_list()
*/
typedef unsigned int uint32;
typedef unsigned int Oid;

typedef struct RelFileNode                                                                                                                                      
{
    Oid         spcNode;        /* tablespace */
    Oid         dbNode;         /* database */
    Oid         relNode;        /* relation */
} RelFileNode;

typedef enum ForkNumber
{
    InvalidForkNumber = -1,
    MAIN_FORKNUM = 0,
    FSM_FORKNUM,
    VISIBILITYMAP_FORKNUM,
    INIT_FORKNUM
} ForkNumber;

typedef uint32 BlockNumber;

typedef struct buftag
{
    RelFileNode rnode;          /* physical relation identifier */
    ForkNumber  forkNum;
    BlockNumber blockNum;       /* blknum relative to begin of reln */
} BufferTag;  

/*
    extern founction for PolarDB
*/
const char* get_progname(const char *argv0);
char **save_ps_display_args(int argc, char **argv);
void write_stderr(const char *fmt,...);

static int get_option(int argc, char *argv[], char opt[][MAX_OPTION_VALUE]);
static int bak_control(char bakpath[]);
static int restore_control(char bakpath[]); 
static int read_block_list(BufferTag **blktag);
void BlockRecover(BufferTag *tag, int blocknum);

int main(int argc, char *argv[])
{
    char opt[1][MAX_OPTION_VALUE], bakpath[MAX_FILE_PATH];
    int blocknum = 0;
    BufferTag *blktag;
    
    progname = get_progname(argv[0]);
    if ( get_option(argc, argv, opt) < 1 )
    {
        write_stderr("br -D datadir\n");
        exit(1);
    }
    argv = save_ps_display_args(argc, argv);

    // change current path to datadir. datadir : -D datadir
    if ( chdir(opt[datadir]) != 0 ) 
    {
        write_stderr("Cann't change dir to datadir\n");
        exit(1);
    }

    // backup controlfile
    bak_control(bakpath);

    // get target bocks list from file restore_block_list 
    blocknum = read_block_list(&blktag);
    if (blocknum <= 0)
    {
        write_stderr("block number %d fail\n", blocknum);
        exit(1);
    }

    // start block recover
    BlockRecover(blktag, blocknum);

    // restore controlfile from backup of bak_control()
    restore_control(bakpath);

    free(blktag);
    return 0;
}

/* 
    get target bocks list from file restore_block_list.
    format of restore_block_list :
    blocks number:NUMBER
    dbNode,relNode,blkno
    dbNode,relNode,blkno
    ......
*/
static int read_block_list(BufferTag **blktag)
{
    char name[MAX_FILE_PATH], ch;
    int blocknum = 0, i;
    FILE *file;
    BufferTag *tag;

    snprintf(name, MAX_FILE_PATH, "%s", PG_BLOCKLIST_FILE); 

    if ((file = fopen(name, "r")) == NULL)
    {
        write_stderr("Open block list file %s error\n", name);
        exit(0);
    }
    if ( (fscanf(file, "blocks number:%d%c", &blocknum, &ch)) != 2 || ch != '\n')
    {
        write_stderr("%s, File Header error, read block number fail.\n", name);
        exit(0);
    }

    tag = (BufferTag *) malloc( sizeof(BufferTag) * blocknum);
    *blktag = tag;

    for ( i = 0; i < blocknum; i++)
    {
        if ( (fscanf(file, "%d,%d,%d%c", &(tag[i].rnode.dbNode), &(tag[i].rnode.relNode), &(tag[i].blockNum), &ch)) != 4 || ch != '\n')
        {
            write_stderr("File message error\n", name);
            exit(0);
        }
    }
    fclose(file);
    return blocknum;
}

// restore controlfile from global/pg_control_TIME.bak
static int restore_control(char bakpath[])
{
    char conpath[MAX_FILE_PATH], readbuf[READ_BUF];
    int fdcon, fdbak;
    ssize_t s;

    snprintf(conpath, MAX_FILE_PATH, "%s/%s", PG_CONTROL_PATH, PG_CONTROL_FILE); 

    fdbak = open(bakpath, O_RDONLY);
    if (fdbak < 0)
    {
        write_stderr("Open backup for control file %s error.\n", bakpath);
        write_stderr("You can restore control file by: cp %s %s\n", bakpath, conpath);
        exit(1);
    }

    fdcon = open(conpath, O_CREAT|O_WRONLY|O_SYNC, S_IRUSR|S_IWUSR);
    if (fdcon < 0)
    {
        write_stderr("Restore control file %s error.\n", conpath);
        write_stderr("You can restore control file by: cp %s %s\n", bakpath, conpath);
        close(fdbak);
        exit(1);
    }

    while ( (s=read(fdbak, readbuf, READ_BUF)) > 0 )
    {
        if ( write(fdcon, readbuf, READ_BUF) != s )
        {
            write_stderr("Write %s error.\n", bakpath);
            close(fdbak);
            close(fdcon);
            exit(1);
        }
    }    
    close(fdbak);
    close(fdcon);
    return 1;
}

// backup global/pg_control to global/pg_control_TIME.bak
static int bak_control(char bakpath[])
{
    char conpath[MAX_FILE_PATH], readbuf[READ_BUF];
    int fdcon, fdbak;
    ssize_t s;
    time_t t;

    t = time(0);
    snprintf(conpath, MAX_FILE_PATH, "%s/%s", PG_CONTROL_PATH, PG_CONTROL_FILE); 
    snprintf(bakpath, MAX_FILE_PATH, "%s/%s_%d.bak", PG_CONTROL_PATH, PG_CONTROL_FILE, (int)t);

    fdcon = open(conpath, O_RDONLY);
    if (fdcon < 0)
    {
        write_stderr("Opened control file %s error.\n", conpath);
        exit(1);
    }
    fdbak = open(bakpath, O_CREAT|O_WRONLY|O_SYNC, S_IRUSR|S_IWUSR);
    if (fdbak < 0)
    {
        write_stderr("Create backup for control file error.\n");
        close(fdcon);
        exit(1);
    }
    while ( (s=read(fdcon, readbuf, READ_BUF)) > 0 )
    {
        if ( write(fdbak, readbuf, READ_BUF) != s )
        {
            write_stderr("Write %s error.\n", bakpath);
            close(fdcon);
            close(fdbak);
            exit(1);
        }
    }    
    close(fdcon);
    close(fdbak);
    return 1;
}

// parse command List.  br -D datadir
static int get_option(int argc, char *argv[], char opt[][MAX_OPTION_VALUE])
{
    int n, val = 0;
    for (n = 1; n < argc; n++)
    {
        if ( strncmp(argv[n], "-D", 2) == 0 )
        {
            strncpy(opt[datadir], argv[++n], MAX_OPTION_VALUE);
            val++;
        }
    }
    return val;
}
