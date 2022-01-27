/*-------------------------------------------------------------------------
 *
 * ip.c
 *      IPv6-aware network access.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *      $PostgreSQL: pgsql/src/backend/libpq/ip.c,v 1.43 2009/01/01 17:23:42 momjian Exp $
 *
 * This file and the IPV6 implementation were initially provided by
 * Nigel Kukard <nkukard@lbsd.net>, Linux Based Systems Design
 * http://www.lbsd.net.
 *
 *-------------------------------------------------------------------------
 */

/* This is intended to be used in both frontend and backend, so use c.h */
#include "gtm/gtm_c.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>
#include <sys/file.h>

#include "gtm/gtm_ip.h"


static int range_sockaddr_AF_INET(const struct sockaddr_in * addr,
                       const struct sockaddr_in * netaddr,
                       const struct sockaddr_in * netmask);

#ifdef HAVE_IPV6
static int range_sockaddr_AF_INET6(const struct sockaddr_in6 * addr,
                        const struct sockaddr_in6 * netaddr,
                        const struct sockaddr_in6 * netmask);
#endif


/*
 *    gtm_getaddrinfo_all - get address info for Unix, IPv4 and IPv6 sockets
 */
int
gtm_getaddrinfo_all(const char *hostname, const char *servname,
                   const struct addrinfo * hintp, struct addrinfo ** result)
{
    int            rc;

    /* not all versions of getaddrinfo() zero *result on failure */
    *result = NULL;

    /* NULL has special meaning to getaddrinfo(). */
    rc = getaddrinfo((!hostname || hostname[0] == '\0') ? NULL : hostname,
                     servname, hintp, result);

    return rc;
}


/*
 *    gtm_freeaddrinfo_all - free addrinfo structures for IPv4, IPv6, or Unix
 *
 * Note: the ai_family field of the original hint structure must be passed
 * so that we can tell whether the addrinfo struct was built by the system's
 * getaddrinfo() routine or our own getaddrinfo_unix() routine.  Some versions
 * of getaddrinfo() might be willing to return AF_UNIX addresses, so it's
 * not safe to look at ai_family in the addrinfo itself.
 */
void
gtm_freeaddrinfo_all(int hint_ai_family, struct addrinfo * ai)
{
    {
        /* struct was built by getaddrinfo() */
        if (ai != NULL)
            freeaddrinfo(ai);
    }
}


/*
 *    gtm_getnameinfo_all - get name info for Unix, IPv4 and IPv6 sockets
 *
 * The API of this routine differs from the standard getnameinfo() definition
 * in two ways: first, the addr parameter is declared as sockaddr_storage
 * rather than struct sockaddr, and second, the node and service fields are
 * guaranteed to be filled with something even on failure return.
 */
int
gtm_getnameinfo_all(const struct sockaddr_storage * addr, int salen,
                   char *node, int nodelen,
                   char *service, int servicelen,
                   int flags)
{
    int            rc;

    rc = getnameinfo((const struct sockaddr *) addr, salen,
                     node, nodelen,
                     service, servicelen,
                     flags);

    if (rc != 0)
    {
        if (node)
            strlcpy(node, "???", nodelen);
        if (service)
            strlcpy(service, "???", servicelen);
    }

    return rc;
}

/*
 * gtm_range_sockaddr - is addr within the subnet specified by netaddr/netmask ?
 *
 * Note: caller must already have verified that all three addresses are
 * in the same address family; and AF_UNIX addresses are not supported.
 */
int
gtm_range_sockaddr(const struct sockaddr_storage * addr,
                  const struct sockaddr_storage * netaddr,
                  const struct sockaddr_storage * netmask)
{
    if (addr->ss_family == AF_INET)
        return range_sockaddr_AF_INET((struct sockaddr_in *) addr,
                                      (struct sockaddr_in *) netaddr,
                                      (struct sockaddr_in *) netmask);
#ifdef HAVE_IPV6
    else if (addr->ss_family == AF_INET6)
        return range_sockaddr_AF_INET6((struct sockaddr_in6 *) addr,
                                       (struct sockaddr_in6 *) netaddr,
                                       (struct sockaddr_in6 *) netmask);
#endif
    else
        return 0;
}

static int
range_sockaddr_AF_INET(const struct sockaddr_in * addr,
                       const struct sockaddr_in * netaddr,
                       const struct sockaddr_in * netmask)
{
    if (((addr->sin_addr.s_addr ^ netaddr->sin_addr.s_addr) &
         netmask->sin_addr.s_addr) == 0)
        return 1;
    else
        return 0;
}


#ifdef HAVE_IPV6

static int
range_sockaddr_AF_INET6(const struct sockaddr_in6 * addr,
                        const struct sockaddr_in6 * netaddr,
                        const struct sockaddr_in6 * netmask)
{
    int            i;

    for (i = 0; i < 16; i++)
    {
        if (((addr->sin6_addr.s6_addr[i] ^ netaddr->sin6_addr.s6_addr[i]) &
             netmask->sin6_addr.s6_addr[i]) != 0)
            return 0;
    }

    return 1;
}
#endif   /* HAVE_IPV6 */

/*
 *    gtm_sockaddr_cidr_mask - make a network mask of the appropriate family
 *      and required number of significant bits
 *
 * The resulting mask is placed in *mask, which had better be big enough.
 *
 * Return value is 0 if okay, -1 if not.
 */
int
gtm_sockaddr_cidr_mask(struct sockaddr_storage * mask, char *numbits, int family)
{// #lizard forgives
    long        bits;
    char       *endptr;

    bits = strtol(numbits, &endptr, 10);

    if (*numbits == '\0' || *endptr != '\0')
        return -1;

    switch (family)
    {
        case AF_INET:
            {
                struct sockaddr_in mask4;
                long        maskl;

                if (bits < 0 || bits > 32)
                    return -1;
                /* avoid "x << 32", which is not portable */
                if (bits > 0)
                    maskl = (0xffffffffUL << (32 - (int) bits))
                        & 0xffffffffUL;
                else
                    maskl = 0;
                mask4.sin_addr.s_addr = htonl(maskl);
                memcpy(mask, &mask4, sizeof(mask4));
                break;
            }

#ifdef HAVE_IPV6
        case AF_INET6:
            {
                struct sockaddr_in6 mask6;
                int            i;

                if (bits < 0 || bits > 128)
                    return -1;
                for (i = 0; i < 16; i++)
                {
                    if (bits <= 0)
                        mask6.sin6_addr.s6_addr[i] = 0;
                    else if (bits >= 8)
                        mask6.sin6_addr.s6_addr[i] = 0xff;
                    else
                    {
                        mask6.sin6_addr.s6_addr[i] =
                            (0xff << (8 - (int) bits)) & 0xff;
                    }
                    bits -= 8;
                }
                memcpy(mask, &mask6, sizeof(mask6));
                break;
            }
#endif
        default:
            return -1;
    }

    mask->ss_family = family;
    return 0;
}


#ifdef HAVE_IPV6

/*
 * gtm_promote_v4_to_v6_addr --- convert an AF_INET addr to AF_INET6, using
 *        the standard convention for IPv4 addresses mapped into IPv6 world
 *
 * The passed addr is modified in place; be sure it is large enough to
 * hold the result!  Note that we only worry about setting the fields
 * that gtm_range_sockaddr will look at.
 */
void
gtm_promote_v4_to_v6_addr(struct sockaddr_storage * addr)
{
    struct sockaddr_in addr4;
    struct sockaddr_in6 addr6;
    uint32        ip4addr;

    memcpy(&addr4, addr, sizeof(addr4));
    ip4addr = ntohl(addr4.sin_addr.s_addr);

    memset(&addr6, 0, sizeof(addr6));

    addr6.sin6_family = AF_INET6;

    addr6.sin6_addr.s6_addr[10] = 0xff;
    addr6.sin6_addr.s6_addr[11] = 0xff;
    addr6.sin6_addr.s6_addr[12] = (ip4addr >> 24) & 0xFF;
    addr6.sin6_addr.s6_addr[13] = (ip4addr >> 16) & 0xFF;
    addr6.sin6_addr.s6_addr[14] = (ip4addr >> 8) & 0xFF;
    addr6.sin6_addr.s6_addr[15] = (ip4addr) & 0xFF;

    memcpy(addr, &addr6, sizeof(addr6));
}

/*
 * gtm_promote_v4_to_v6_mask --- convert an AF_INET netmask to AF_INET6, using
 *        the standard convention for IPv4 addresses mapped into IPv6 world
 *
 * This must be different from gtm_promote_v4_to_v6_addr because we want to
 * set the high-order bits to 1's not 0's.
 *
 * The passed addr is modified in place; be sure it is large enough to
 * hold the result!  Note that we only worry about setting the fields
 * that gtm_range_sockaddr will look at.
 */
void
gtm_promote_v4_to_v6_mask(struct sockaddr_storage * addr)
{
    struct sockaddr_in addr4;
    struct sockaddr_in6 addr6;
    uint32        ip4addr;
    int            i;

    memcpy(&addr4, addr, sizeof(addr4));
    ip4addr = ntohl(addr4.sin_addr.s_addr);

    memset(&addr6, 0, sizeof(addr6));

    addr6.sin6_family = AF_INET6;

    for (i = 0; i < 12; i++)
        addr6.sin6_addr.s6_addr[i] = 0xff;

    addr6.sin6_addr.s6_addr[12] = (ip4addr >> 24) & 0xFF;
    addr6.sin6_addr.s6_addr[13] = (ip4addr >> 16) & 0xFF;
    addr6.sin6_addr.s6_addr[14] = (ip4addr >> 8) & 0xFF;
    addr6.sin6_addr.s6_addr[15] = (ip4addr) & 0xFF;

    memcpy(addr, &addr6, sizeof(addr6));
}

#endif   /* HAVE_IPV6 */
