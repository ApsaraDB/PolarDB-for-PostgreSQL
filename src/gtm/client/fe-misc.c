/*-------------------------------------------------------------------------
 *
 *     FILE
 *        fe-misc.c
 *
 *     DESCRIPTION
 *         miscellaneous useful functions
 *
 * The communication routines here are analogous to the ones in
 * backend/libpq/pqcomm.c and backend/libpq/pqcomprim.c, but operate
 * in the considerably different environment of the frontend libpq.
 * In particular, we work with a bare nonblock-mode socket, rather than
 * a stdio stream, so that we can avoid unwanted blocking of the application.
 *
 * XXX: MOVE DEBUG PRINTOUT TO HIGHER LEVEL.  As is, block and restart
 * will cause repeat printouts.
 *
 * We must speak the same transmitted data representations as the backend
 * routines.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *      $PostgreSQL: pgsql/src/interfaces/libpq/fe-misc.c,v 1.137 2008/12/11 07:34:09 petere Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm_c.h"

#include <signal.h>
#include <time.h>

#include <netinet/in.h>
#include <arpa/inet.h>

#include <unistd.h>
#include <sys/time.h>

#include <poll.h>
#include <sys/poll.h>
#include <sys/select.h>

#include "gtm/libpq-fe.h"
#include "gtm/libpq-int.h"

static int    gtmpqPutMsgBytes(const void *buf, size_t len, GTM_Conn *conn);
static int    gtmpqSendSome(GTM_Conn *conn, int len);
static int gtmpqSocketCheck(GTM_Conn *conn, int forRead, int forWrite,
              time_t end_time);
static int    gtmpqSocketPoll(int sock, int forRead, int forWrite, time_t end_time);


/*
 * gtmpqGetc: get 1 character from the connection
 *
 *    All these routines return 0 on success, EOF on error.
 *    Note that for the Get routines, EOF only means there is not enough
 *    data in the buffer, not that there is necessarily a hard error.
 */
int
gtmpqGetc(char *result, GTM_Conn *conn)
{
    if (conn->inCursor >= conn->inEnd)
        return EOF;

    *result = conn->inBuffer[conn->inCursor++];

    if (conn->Pfdebug)
        fprintf(conn->Pfdebug, "From backend> %c\n", *result);

    return 0;
}


/*
 * gtmpqPutc: write 1 char to the current message
 */
int
gtmpqPutc(char c, GTM_Conn *conn)
{
    if (gtmpqPutMsgBytes(&c, 1, conn))
        return EOF;

    if (conn->Pfdebug)
        fprintf(conn->Pfdebug, "To backend> %c\n", c);

    return 0;
}


/*
 * gtmpqGets[_append]:
 * get a null-terminated string from the connection,
 * and store it in an expansible PQExpBuffer.
 * If we run out of memory, all of the string is still read,
 * but the excess characters are silently discarded.
 */
static int
gtmpqGets_internal(PQExpBuffer buf, GTM_Conn *conn, bool resetbuffer)
{
    /* Copy conn data to locals for faster search loop */
    char       *inBuffer = conn->inBuffer;
    int            inCursor = conn->inCursor;
    int            inEnd = conn->inEnd;
    int            slen;

    while (inCursor < inEnd && inBuffer[inCursor])
        inCursor++;

    if (inCursor >= inEnd)
        return EOF;

    slen = inCursor - conn->inCursor;

    if (resetbuffer)
        resetGTMPQExpBuffer(buf);

    appendBinaryGTMPQExpBuffer(buf, inBuffer + conn->inCursor, slen);

    conn->inCursor = ++inCursor;

    if (conn->Pfdebug)
        fprintf(conn->Pfdebug, "From backend> \"%s\"\n",
                buf->data);

    return 0;
}

int
gtmpqGets(PQExpBuffer buf, GTM_Conn *conn)
{
    return gtmpqGets_internal(buf, conn, true);
}

int
gtmpqGets_append(PQExpBuffer buf, GTM_Conn *conn)
{
    return gtmpqGets_internal(buf, conn, false);
}


/*
 * gtmpqPuts: write a null-terminated string to the current message
 */
int
gtmpqPuts(const char *s, GTM_Conn *conn)
{
    if (gtmpqPutMsgBytes(s, strlen(s) + 1, conn))
        return EOF;

    if (conn->Pfdebug)
        fprintf(conn->Pfdebug, "To backend> \"%s\"\n", s);

    return 0;
}

/*
 * gtmpqGetnchar:
 *    get a string of exactly len bytes in buffer s, no null termination
 */
int
gtmpqGetnchar(char *s, size_t len, GTM_Conn *conn)
{
    if (len > (size_t) (conn->inEnd - conn->inCursor))
        return EOF;

    memcpy(s, conn->inBuffer + conn->inCursor, len);
    /* no terminating null */

    conn->inCursor += len;

    if (conn->Pfdebug)
        fprintf(conn->Pfdebug, "From backend (%lu)> %.*s\n",
                (unsigned long) len, (int) len, s);

    return 0;
}

/*
 * gtmpqPutnchar:
 *    write exactly len bytes to the current message
 */
int
gtmpqPutnchar(const char *s, size_t len, GTM_Conn *conn)
{
    if (gtmpqPutMsgBytes(s, len, conn))
        return EOF;

    if (conn->Pfdebug)
        fprintf(conn->Pfdebug, "To backend> %.*s\n", (int) len, s);

    return 0;
}

/*
 * gtmpqGetInt
 *    read a 2 or 4 byte integer and convert from network byte order
 *    to local byte order
 */
int
gtmpqGetInt(int *result, size_t bytes, GTM_Conn *conn)
{
    uint16        tmp2;
    uint32        tmp4;

    switch (bytes)
    {
        case 2:
            if (conn->inCursor + 2 > conn->inEnd)
                return EOF;
            memcpy(&tmp2, conn->inBuffer + conn->inCursor, 2);
            conn->inCursor += 2;
            *result = (int) ntohs(tmp2);
            break;
        case 4:
            if (conn->inCursor + 4 > conn->inEnd)
                return EOF;
            memcpy(&tmp4, conn->inBuffer + conn->inCursor, 4);
            conn->inCursor += 4;
            *result = (int) ntohl(tmp4);
            break;
        default:
            fprintf(conn->Pfdebug, "Integer size of (%ld) bytes not supported", bytes);
            return EOF;
    }

    if (conn->Pfdebug)
        fprintf(conn->Pfdebug, "From backend (#%lu)> %d\n", (unsigned long) bytes, *result);

    return 0;
}

/*
 * gtmpqPutInt
 * write an integer of 2 or 4 bytes, converting from host byte order
 * to network byte order.
 */
int
gtmpqPutInt(int value, size_t bytes, GTM_Conn *conn)
{
    uint16        tmp2;
    uint32        tmp4;

    switch (bytes)
    {
        case 2:
            tmp2 = htons((uint16) value);
            if (gtmpqPutMsgBytes((const char *) &tmp2, 2, conn))
                return EOF;
            break;
        case 4:
            tmp4 = htonl((uint32) value);
            if (gtmpqPutMsgBytes((const char *) &tmp4, 4, conn))
                return EOF;
            break;
        default:
            fprintf(conn->Pfdebug, "Integer size of (%ld) bytes not supported", bytes);
            return EOF;
    }

    if (conn->Pfdebug)
        fprintf(conn->Pfdebug, "To backend (%lu#)> %d\n", (unsigned long) bytes, value);

    return 0;
}

/*
 * Make sure conn's output buffer can hold bytes_needed bytes (caller must
 * include already-stored data into the value!)
 *
 * Returns 0 on success, EOF if failed to enlarge buffer
 */
int
gtmpqCheckOutBufferSpace(size_t bytes_needed, GTM_Conn *conn)
{// #lizard forgives
    int            newsize = conn->outBufSize;
    char       *newbuf;

    if (bytes_needed <= (size_t) newsize)
        return 0;

    /*
     * If we need to enlarge the buffer, we first try to double it in size; if
     * that doesn't work, enlarge in multiples of 8K.  This avoids thrashing
     * the malloc pool by repeated small enlargements.
     *
     * Note: tests for newsize > 0 are to catch integer overflow.
     */
    do
    {
        newsize *= 2;
    } while (newsize > 0 && bytes_needed > (size_t) newsize);

    if (newsize > 0 && bytes_needed <= (size_t) newsize)
    {
        newbuf = realloc(conn->outBuffer, newsize);
        if (newbuf)
        {
            /* realloc succeeded */
            conn->outBuffer = newbuf;
            conn->outBufSize = newsize;
            return 0;
        }
    }

    newsize = conn->outBufSize;
    do
    {
        newsize += 8192;
    } while (newsize > 0 && bytes_needed > (size_t) newsize);

    if (newsize > 0 && bytes_needed <= (size_t) newsize)
    {
        newbuf = realloc(conn->outBuffer, newsize);
        if (newbuf)
        {
            /* realloc succeeded */
            conn->outBuffer = newbuf;
            conn->outBufSize = newsize;
            return 0;
        }
    }

    /* realloc failed. Probably out of memory */
    printfGTMPQExpBuffer(&conn->errorMessage,
                      "cannot allocate memory for output buffer\n");
    return EOF;
}

/*
 * Make sure conn's input buffer can hold bytes_needed bytes (caller must
 * include already-stored data into the value!)
 *
 * Returns 0 on success, EOF if failed to enlarge buffer
 */
int
gtmpqCheckInBufferSpace(size_t bytes_needed, GTM_Conn *conn)
{// #lizard forgives
    int            newsize = conn->inBufSize;
    char       *newbuf;

    if (bytes_needed <= (size_t) newsize)
        return 0;

    /*
     * If we need to enlarge the buffer, we first try to double it in size; if
     * that doesn't work, enlarge in multiples of 8K.  This avoids thrashing
     * the malloc pool by repeated small enlargements.
     *
     * Note: tests for newsize > 0 are to catch integer overflow.
     */
    do
    {
        newsize *= 2;
    } while (newsize > 0 && bytes_needed > (size_t) newsize);

    if (newsize > 0 && bytes_needed <= (size_t) newsize)
    {
        newbuf = realloc(conn->inBuffer, newsize);
        if (newbuf)
        {
            /* realloc succeeded */
            conn->inBuffer = newbuf;
            conn->inBufSize = newsize;
            return 0;
        }
    }

    newsize = conn->inBufSize;
    do
    {
        newsize += 8192;
    } while (newsize > 0 && bytes_needed > (size_t) newsize);

    if (newsize > 0 && bytes_needed <= (size_t) newsize)
    {
        newbuf = realloc(conn->inBuffer, newsize);
        if (newbuf)
        {
            /* realloc succeeded */
            conn->inBuffer = newbuf;
            conn->inBufSize = newsize;
            return 0;
        }
    }

    /* realloc failed. Probably out of memory */
    printfGTMPQExpBuffer(&conn->errorMessage,
                      "cannot allocate memory for input buffer\n");
    return EOF;
}

/*
 * gtmpqPutMsgStart: begin construction of a message to the server
 *
 * msg_type is the message type byte, or 0 for a message without type byte
 * (only startup messages have no type byte)
 *
 * force_len forces the message to have a length word; otherwise, we add
 * a length word if protocol 3.
 *
 * Returns 0 on success, EOF on error
 *
 * The idea here is that we construct the message in conn->outBuffer,
 * beginning just past any data already in outBuffer (ie, at
 * outBuffer+outCount).  We enlarge the buffer as needed to hold the message.
 * When the message is complete, we fill in the length word (if needed) and
 * then advance outCount past the message, making it eligible to send.
 *
 * The state variable conn->outMsgStart points to the incomplete message's
 * length word: it is either outCount or outCount+1 depending on whether
 * there is a type byte.  If we are sending a message without length word
 * (pre protocol 3.0 only), then outMsgStart is -1.  The state variable
 * conn->outMsgEnd is the end of the data collected so far.
 */
int
gtmpqPutMsgStart(char msg_type, bool force_len, GTM_Conn *conn)
{
    int            lenPos;
    int            endPos;

    /* allow room for message type byte */
    if (msg_type)
        endPos = conn->outCount + 1;
    else
        endPos = conn->outCount;

    /* do we want a length word? */
    if (force_len)
    {
        lenPos = endPos;
        /* allow room for message length */
        endPos += 4;
    }
    else
        lenPos = -1;

    /* make sure there is room for message header */
    if (gtmpqCheckOutBufferSpace(endPos, conn))
        return EOF;
    /* okay, save the message type byte if any */
    if (msg_type)
        conn->outBuffer[conn->outCount] = msg_type;
    /* set up the message pointers */
    conn->outMsgStart = lenPos;
    conn->outMsgEnd = endPos;
    /* length word, if needed, will be filled in by gtmpqPutMsgEnd */

    if (conn->Pfdebug)
        fprintf(conn->Pfdebug, "To backend> Msg %c\n",
                msg_type ? msg_type : ' ');

    return 0;
}

/*
 * gtmpqPutMsgBytes: add bytes to a partially-constructed message
 *
 * Returns 0 on success, EOF on error
 */
static int
gtmpqPutMsgBytes(const void *buf, size_t len, GTM_Conn *conn)
{
    /* make sure there is room for it */
    if (gtmpqCheckOutBufferSpace(conn->outMsgEnd + len, conn))
        return EOF;
    /* okay, save the data */
    memcpy(conn->outBuffer + conn->outMsgEnd, buf, len);
    conn->outMsgEnd += len;
    /* no Pfdebug call here, caller should do it */
    return 0;
}

/*
 * gtmpqPutMsgEnd: finish constructing a message and possibly send it
 *
 * Returns 0 on success, EOF on error
 *
 * We don't actually send anything here unless we've accumulated at least
 * 8K worth of data (the typical size of a pipe buffer on Unix systems).
 * This avoids sending small partial packets.  The caller must use gtmpqFlush
 * when it's important to flush all the data out to the server.
 */
int
gtmpqPutMsgEnd(GTM_Conn *conn)
{
    if (conn->Pfdebug)
        fprintf(conn->Pfdebug, "To backend> Msg complete, length %u\n",
                conn->outMsgEnd - conn->outCount);

    /* Fill in length word if needed */
    if (conn->outMsgStart >= 0)
    {
        uint32        msgLen = conn->outMsgEnd - conn->outMsgStart;

        msgLen = htonl(msgLen);
        memcpy(conn->outBuffer + conn->outMsgStart, &msgLen, 4);
    }

    /* Make message eligible to send */
    conn->outCount = conn->outMsgEnd;

    if (conn->outCount >= 8192)
    {
        int            toSend = conn->outCount - (conn->outCount % 8192);

        if (gtmpqSendSome(conn, toSend) < 0)
            return EOF;
        /* in nonblock mode, don't complain if unable to send it all */
    }

    return 0;
}

/* ----------
 * gtmpqReadData: read more data, if any is available
 * Possible return values:
 *     1: successfully loaded at least one more byte
 *     0: no data is presently available, but no error detected
 *    -1: error detected (including EOF = connection closure);
 *        conn->errorMessage set
 * NOTE: callers must not assume that pointers or indexes into conn->inBuffer
 * remain valid across this call!
 * ----------
 */
int
gtmpqReadData(GTM_Conn *conn)
{// #lizard forgives
    int            someread = 0;
    int            nread;

    if (conn->sock < 0)
    {
        printfGTMPQExpBuffer(&conn->errorMessage,
                          "connection not open\n");
        return -1;
    }

    /* Left-justify any data in the buffer to make room */
    if (conn->inStart < conn->inEnd)
    {
        if (conn->inStart > 0)
        {
            memmove(conn->inBuffer, conn->inBuffer + conn->inStart,
                    conn->inEnd - conn->inStart);
            conn->inEnd -= conn->inStart;
            conn->inCursor -= conn->inStart;
            conn->inStart = 0;
        }
    }
    else
    {
        /* buffer is logically empty, reset it */
        conn->inStart = conn->inCursor = conn->inEnd = 0;
    }

    /*
     * If the buffer is fairly full, enlarge it. We need to be able to enlarge
     * the buffer in case a single message exceeds the initial buffer size. We
     * enlarge before filling the buffer entirely so as to avoid asking the
     * kernel for a partial packet. The magic constant here should be large
     * enough for a TCP packet or Unix pipe bufferload.  8K is the usual pipe
     * buffer size, so...
     */
    if (conn->inBufSize - conn->inEnd < 8192)
    {
        if (gtmpqCheckInBufferSpace(conn->inEnd + (size_t) 8192, conn))
        {
            /*
             * We don't insist that the enlarge worked, but we need some room
             */
            if (conn->inBufSize - conn->inEnd < 100)
                return -1;        /* errorMessage already set */
        }
    }

    /* OK, try to read some data */
retry3:
    nread = recv(conn->sock, conn->inBuffer + conn->inEnd,
                          conn->inBufSize - conn->inEnd, 0);
    conn->last_call = GTM_LastCall_RECV;
    if (nread < 0)
    {
        conn->last_errno = SOCK_ERRNO;

        if (SOCK_ERRNO == EINTR)
            goto retry3;
        /* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
        if (SOCK_ERRNO == EAGAIN)
            return someread;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
        if (SOCK_ERRNO == EWOULDBLOCK)
            return someread;
#endif
        /* We might get ECONNRESET here if using TCP and backend died */
#ifdef ECONNRESET
        if (SOCK_ERRNO == ECONNRESET)
            goto definitelyFailed;
#endif
        printfGTMPQExpBuffer(&conn->errorMessage,
                   "could not receive data from server:\n");
        return -1;
    }
    else
        conn->last_errno = 0;

    if (nread > 0)
    {
        conn->inEnd += nread;

        /*
         * Hack to deal with the fact that some kernels will only give us back
         * 1 packet per recv() call, even if we asked for more and there is
         * more available.    If it looks like we are reading a long message,
         * loop back to recv() again immediately, until we run out of data or
         * buffer space.  Without this, the block-and-restart behavior of
         * libpq's higher levels leads to O(N^2) performance on long messages.
         *
         * Since we left-justified the data above, conn->inEnd gives the
         * amount of data already read in the current message.    We consider
         * the message "long" once we have acquired 32k ...
         */
#ifdef NOT_USED
        if (conn->inEnd > 32768 &&
            (conn->inBufSize - conn->inEnd) >= 8192)
        {
            someread = 1;
            goto retry3;
        }
#endif
        return 1;
    }

    if (someread)
        return 1;                /* got a zero read after successful tries */

    /*
     * A return value of 0 could mean just that no data is now available, or
     * it could mean EOF --- that is, the server has closed the connection.
     * Since we have the socket in nonblock mode, the only way to tell the
     * difference is to see if select() is saying that the file is ready.
     * Grumble.  Fortunately, we don't expect this path to be taken much,
     * since in normal practice we should not be trying to read data unless
     * the file selected for reading already.
     *
     * In SSL mode it's even worse: SSL_read() could say WANT_READ and then
     * data could arrive before we make the gtmpqReadReady() test.  So we must
     * play dumb and assume there is more data, relying on the SSL layer to
     * detect true EOF.
     */

    switch (gtmpqReadReady(conn))
    {
        case 0:
            /* definitely no data available */
            return 0;
        case 1:
            /* ready for read */
            break;
        default:
            goto definitelyFailed;
    }

    /*
     * Still not sure that it's EOF, because some data could have just
     * arrived.
     */
retry4:
    nread = recv(conn->sock, conn->inBuffer + conn->inEnd,
                          conn->inBufSize - conn->inEnd, 0);
    conn->last_call = GTM_LastCall_RECV;
    if (nread < 0)
    {
        conn->last_errno = SOCK_ERRNO;
        if (SOCK_ERRNO == EINTR)
            goto retry4;
        /* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
        if (SOCK_ERRNO == EAGAIN)
            return 0;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
        if (SOCK_ERRNO == EWOULDBLOCK)
            return 0;
#endif
        /* We might get ECONNRESET here if using TCP and backend died */
#ifdef ECONNRESET
        if (SOCK_ERRNO == ECONNRESET)
            goto definitelyFailed;
#endif
        printfGTMPQExpBuffer(&conn->errorMessage,
                   "could not receive data from server: \n");
        return -1;
    }
    else
        conn->last_errno = 0;
    if (nread > 0)
    {
        conn->inEnd += nread;
        return 1;
    }

    /*
     * OK, we are getting a zero read even though select() says ready. This
     * means the connection has been closed.  Cope.
     */
definitelyFailed:
    printfGTMPQExpBuffer(&conn->errorMessage,
                                "server closed the connection unexpectedly\n"
                   "\tThis probably means the server terminated abnormally\n"
                             "\tbefore or while processing the request.\n");
    conn->status = CONNECTION_BAD;        /* No more connection to backend */
    close(conn->sock);
    conn->sock = -1;

    return -1;
}

/*
 * gtmpqSendSome: send data waiting in the output buffer.
 *
 * len is how much to try to send (typically equal to outCount, but may
 * be less).
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 */
static int
gtmpqSendSome(GTM_Conn *conn, int len)
{// #lizard forgives
    char       *ptr = conn->outBuffer;
    int            remaining = conn->outCount;
    int            result = 0;

    if (conn->sock < 0)
    {
        printfGTMPQExpBuffer(&conn->errorMessage,
                          "connection not open\n");
        return -1;
    }

    /* while there's still data to send */
    while (len > 0)
    {
        int            sent;

        sent = send(conn->sock, ptr, len, 0);
        conn->last_call = GTM_LastCall_SEND;

        if (sent < 0)
        {
            conn->last_errno = SOCK_ERRNO;
            /*
             * Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble. If it's
             * EPIPE or ECONNRESET, assume we've lost the backend connection
             * permanently.
             */
            switch (SOCK_ERRNO)
            {
#ifdef EAGAIN
                case EAGAIN:
                    break;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
                case EWOULDBLOCK:
                    break;
#endif
                case EINTR:
                    continue;

                case EPIPE:
#ifdef ECONNRESET
                case ECONNRESET:
#endif
                    printfGTMPQExpBuffer(&conn->errorMessage,
                                "server closed the connection unexpectedly\n"
                    "\tThis probably means the server terminated abnormally\n"
                             "\tbefore or while processing the request.\n");

                    /*
                     * We used to close the socket here, but that's a bad idea
                     * since there might be unread data waiting (typically, a
                     * NOTICE message from the backend telling us it's
                     * committing hara-kiri...).  Leave the socket open until
                     * gtmpqReadData finds no more data can be read.  But abandon
                     * attempt to send data.
                     */
                    conn->outCount = 0;
                    return -1;

                default:
                    printfGTMPQExpBuffer(&conn->errorMessage,
                        "could not send data to server: \n");
                    /* We don't assume it's a fatal error... */
                    conn->outCount = 0;
                    return -1;
            }
        }
        else
        {
            ptr += sent;
            len -= sent;
            remaining -= sent;
            conn->last_errno = 0;
        }

        if (len > 0)
        {
            /*
             * We didn't send it all, wait till we can send more.
             *
             * If the connection is in non-blocking mode we don't wait, but
             * return 1 to indicate that data is still pending.
             */
            result = 1;
            break;
        }
    }

    /* shift the remaining contents of the buffer */
    if (remaining > 0)
        memmove(conn->outBuffer, ptr, remaining);
    conn->outCount = remaining;

    return result;
}


/*
 * gtmpqFlush: send any data waiting in the output buffer
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 */
int
gtmpqFlush(GTM_Conn *conn)
{
    if (conn->Pfdebug)
        fflush(conn->Pfdebug);

    if (conn->outCount > 0)
        return gtmpqSendSome(conn, conn->outCount);

    return 0;
}


/*
 * gtmpqWait: wait until we can read or write the connection socket
 *
 * JAB: If SSL enabled and used and forRead, buffered bytes short-circuit the
 * call to select().
 *
 * We also stop waiting and return if the kernel flags an exception condition
 * on the socket.  The actual error condition will be detected and reported
 * when the caller tries to read or write the socket.
 */
int
gtmpqWait(int forRead, int forWrite, GTM_Conn *conn)
{
    return gtmpqWaitTimed(forRead, forWrite, conn, (time_t) -1);
}

/*
 * gtmpqWaitTimed: wait, but not past finish_time.
 *
 * If finish_time is exceeded then we return failure (EOF).  This is like
 * the response for a kernel exception because we don't want the caller
 * to try to read/write in that case.
 *
 * finish_time = ((time_t) -1) disables the wait limit.
 */
int
gtmpqWaitTimed(int forRead, int forWrite, GTM_Conn *conn, time_t finish_time)
{
    int            result;

    result = gtmpqSocketCheck(conn, forRead, forWrite, finish_time);

    if (result < 0)
        return EOF;                /* errorMessage is already set */

    if (result == 0)
    {
        printfGTMPQExpBuffer(&conn->errorMessage,
                          "timeout expired\n");
        return EOF;
    }

    return 0;
}

/*
 * gtmpqReadReady: is select() saying the file is ready to read?
 * Returns -1 on failure, 0 if not ready, 1 if ready.
 */
int
gtmpqReadReady(GTM_Conn *conn)
{
    return gtmpqSocketCheck(conn, 1, 0, (time_t) 0);
}

/*
 * gtmpqWriteReady: is select() saying the file is ready to write?
 * Returns -1 on failure, 0 if not ready, 1 if ready.
 */
int
gtmpqWriteReady(GTM_Conn *conn)
{
    return gtmpqSocketCheck(conn, 0, 1, (time_t) 0);
}

/*
 * Checks a socket, using poll or select, for data to be read, written,
 * or both.  Returns >0 if one or more conditions are met, 0 if it timed
 * out, -1 if an error occurred.
 *
 * If SSL is in use, the SSL buffer is checked prior to checking the socket
 * for read data directly.
 */
static int
gtmpqSocketCheck(GTM_Conn *conn, int forRead, int forWrite, time_t end_time)
{
    int            result;

    if (!conn)
        return -1;
    if (conn->sock < 0)
    {
        printfGTMPQExpBuffer(&conn->errorMessage,
                          "socket not open\n");
        return -1;
    }

    /* We will retry as long as we get EINTR */
    do
    {
        result = gtmpqSocketPoll(conn->sock, forRead, forWrite, end_time);
        
    }while (result < 0 && SOCK_ERRNO == EINTR);

    if (result < 0)
        printfGTMPQExpBuffer(&conn->errorMessage,
                          "------------ poll() failed: res %d errno %d in gtmpqSocketCheck -----------\n", result, SOCK_ERRNO);

    return result;
}


/*
 * Check a file descriptor for read and/or write data, possibly waiting.
 * If neither forRead nor forWrite are set, immediately return a timeout
 * condition (without waiting).  Return >0 if condition is met, 0
 * if a timeout occurred, -1 if an error or interrupt occurred.
 *
 * Timeout is infinite if end_time is -1.  Timeout is immediate (no blocking)
 * if end_time is 0 (or indeed, any time before now).
 */
static int
gtmpqSocketPoll(int sock, int forRead, int forWrite, time_t end_time)
{// #lizard forgives
    /* We use poll(2) if available, otherwise select(2) */
#ifdef HAVE_POLL
    struct pollfd input_fd;
    int            timeout_ms;

    if (!forRead && !forWrite)
        return 0;

    input_fd.fd = sock;
    input_fd.events = POLLERR;
    input_fd.revents = 0;

    if (forRead)
        input_fd.events |= POLLIN;
    if (forWrite)
        input_fd.events |= POLLOUT;

    /* Compute appropriate timeout interval */
    if (end_time == ((time_t) -1))
        timeout_ms = -1;
    else
    {
        time_t        now = time(NULL);

        if (end_time > now)
            timeout_ms = (end_time - now) * 1000;
        else
            timeout_ms = 0;
    }

    return poll(&input_fd, 1, timeout_ms);
#else                            /* !HAVE_POLL */

    fd_set        input_mask;
    fd_set        output_mask;
    fd_set        except_mask;
    struct timeval timeout;
    struct timeval *ptr_timeout;

    if (!forRead && !forWrite)
        return 0;

    FD_ZERO(&input_mask);
    FD_ZERO(&output_mask);
    FD_ZERO(&except_mask);
    if (forRead)
        FD_SET(sock, &input_mask);
    if (forWrite)
        FD_SET(sock, &output_mask);
    FD_SET(sock, &except_mask);

    /* Compute appropriate timeout interval */
    if (end_time == ((time_t) -1))
        ptr_timeout = NULL;
    else
    {
        time_t        now = time(NULL);

        if (end_time > now)
            timeout.tv_sec = end_time - now;
        else
            timeout.tv_sec = 0;
        timeout.tv_usec = 0;
        ptr_timeout = &timeout;
    }

    return select(sock + 1, &input_mask, &output_mask,
                  &except_mask, ptr_timeout);
#endif   /* HAVE_POLL */
}

#ifdef POLARDB_X
/*
 * gtmpqGetInt8
 *    read a 8 byte integer and convert from network byte order
 *    to local byte order
 */
int
gtmpqGetInt64(int64 *result, GTM_Conn *conn)
{
    int64       value;
    uint32      high;
    uint32      low;
    
    /* high first */
    if (gtmpqGetInt((int32*)&high, sizeof (uint32), conn))
    {
        return -1;
    }
    
    if (gtmpqGetInt((int32*)&low, sizeof (uint32), conn))
    {
        return -1;
    }
    value = high;
    value <<= 32; 
    value |= low;
    
    *result = value;
    return 0;
}

int
gtmpqPutInt64(int64 value, GTM_Conn *conn)
{
    int32      high = value >> 32;
    int32      low  = value & 0xFFFFFFFF;
    
    /* high first */
    if (gtmpqPutInt(high, sizeof (uint32), conn))
        return -1;
    
    if (gtmpqPutInt(low, sizeof (uint32), conn))
        return -1;
    
    return 0;
}

bool
gtmpqHasDataLeft(GTM_Conn *conn)
{
    return  conn->inCursor >= conn->inEnd ? false : true;
}
#endif
