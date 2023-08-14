/*-------------------------------------------------------------------------
 *
 * libpq-be.h
 *	  This file contains definitions for structures and externs used
 *	  by the postmaster during client authentication.
 *
 *	  Note that this is backend-internal and is NOT exported to clients.
 *	  Structs that need to be client-visible are in pqcomm.h.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/libpq-be.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LIBPQ_BE_H
#define LIBPQ_BE_H

#include <sys/time.h>
#ifdef USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif

#ifdef ENABLE_GSS
#if defined(HAVE_GSSAPI_H)
#include <gssapi.h>
#else
#include <gssapi/gssapi.h>
#endif							/* HAVE_GSSAPI_H */
/*
 * GSSAPI brings in headers that set a lot of things in the global namespace on win32,
 * that doesn't match the msvc build. It gives a bunch of compiler warnings that we ignore,
 * but also defines a symbol that simply does not exist. Undefine it again.
 */
#ifdef _MSC_VER
#undef HAVE_GETADDRINFO
#endif
#endif							/* ENABLE_GSS */

#ifdef ENABLE_SSPI
#define SECURITY_WIN32
#if defined(WIN32) && !defined(_MSC_VER)
#include <ntsecapi.h>
#endif
#include <security.h>
#undef SECURITY_WIN32

#ifndef ENABLE_GSS
/*
 * Define a fake structure compatible with GSSAPI on Unix.
 */
typedef struct
{
	void	   *value;
	int			length;
} gss_buffer_desc;
#endif
#endif							/* ENABLE_SSPI */

#include "datatype/timestamp.h"
#include "libpq/hba.h"
#include "libpq/pqcomm.h"


typedef enum CAC_state
{
	CAC_OK, CAC_STARTUP, CAC_SHUTDOWN, CAC_RECOVERY, CAC_TOOMANY,
	CAC_WAITBACKUP
} CAC_state;


/*
 * GSSAPI specific state information
 */
#if defined(ENABLE_GSS) | defined(ENABLE_SSPI)
typedef struct
{
	gss_buffer_desc outbuf;		/* GSSAPI output token buffer */
#ifdef ENABLE_GSS
	gss_cred_id_t cred;			/* GSSAPI connection cred's */
	gss_ctx_id_t ctx;			/* GSSAPI connection context */
	gss_name_t	name;			/* GSSAPI client name */
#endif
} pg_gssinfo;
#endif

/*
 * This is used by the postmaster in its communication with frontends.  It
 * contains all state information needed during this communication before the
 * backend is run.  The Port structure is kept in malloc'd memory and is
 * still available when a backend is running (see MyProcPort).  The data
 * it points to must also be malloc'd, or else palloc'd in TopMemoryContext,
 * so that it survives into PostgresMain execution!
 *
 * remote_hostname is set if we did a successful reverse lookup of the
 * client's IP address during connection setup.
 * remote_hostname_resolv tracks the state of hostname verification:
 *	+1 = remote_hostname is known to resolve to client's IP address
 *	-1 = remote_hostname is known NOT to resolve to client's IP address
 *	 0 = we have not done the forward DNS lookup yet
 *	-2 = there was an error in name resolution
 * If reverse lookup of the client IP address fails, remote_hostname will be
 * left NULL while remote_hostname_resolv is set to -2.  If reverse lookup
 * succeeds but forward lookup fails, remote_hostname_resolv is also set to -2
 * (the case is distinguishable because remote_hostname isn't NULL).  In
 * either of the -2 cases, remote_hostname_errcode saves the lookup return
 * code for possible later use with gai_strerror.
 */

typedef struct Port
{
	pgsocket	sock;			/* File descriptor */
	bool		noblock;		/* is the socket in non-blocking mode? */
	ProtocolVersion proto;		/* FE/BE protocol version */
	SockAddr	laddr;			/* local addr (postmaster) */
	SockAddr	raddr;			/* remote addr (client) */
	char	   *remote_host;	/* name (or ip addr) of remote host */
	char	   *remote_hostname;	/* name (not ip addr) of remote host, if
									 * available */
	int			remote_hostname_resolv; /* see above */
	int			remote_hostname_errcode;	/* see above */
	char	   *remote_port;	/* text rep of remote port */
	CAC_state	canAcceptConnections;	/* postmaster connection status */

	/*
	 * Information that needs to be saved from the startup packet and passed
	 * into backend execution.  "char *" fields are NULL if not set.
	 * guc_options points to a List of alternating option names and values.
	 */
	char	   *database_name;
	char	   *user_name;
	char	   *cmdline_options;
	List	   *guc_options;

	/*
	 * Information that needs to be held during the authentication cycle.
	 */
	HbaLine    *hba;

	/*
	 * Information that really has no business at all being in struct Port,
	 * but since it gets used by elog.c in the same way as database_name and
	 * other members of this struct, we may as well keep it here.
	 */
	TimestampTz SessionStartTime;	/* backend start time */

	/*
	 * TCP keepalive settings.
	 *
	 * default values are 0 if AF_UNIX or not yet known; current values are 0
	 * if AF_UNIX or using the default. Also, -1 in a default value means we
	 * were unable to find out the default (getsockopt failed).
	 */
	int			default_keepalives_idle;
	int			default_keepalives_interval;
	int			default_keepalives_count;
	int			keepalives_idle;
	int			keepalives_interval;
	int			keepalives_count;

#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)

	/*
	 * If GSSAPI is supported, store GSSAPI information. Otherwise, store a
	 * NULL pointer to make sure offsets in the struct remain the same.
	 */
	pg_gssinfo *gss;
#else
	void	   *gss;
#endif

	/*
	 * SSL structures.
	 */
	bool		ssl_in_use;
	char	   *peer_cn;
	bool		peer_cert_valid;

	/*
	 * OpenSSL structures. (Keep these last so that the locations of other
	 * fields are the same whether or not you build with OpenSSL.)
	 */
#ifdef USE_OPENSSL
	SSL		   *ssl;
	X509	   *peer;
#endif

	/* POLAR: Information about origin connection */
	bool 		polar_proxy;
	bool		polar_send_lsn;
	bool 		polar_send_xact;
	bool		polar_proxy_ssl_in_use;
	SockAddr 	polar_origin_addr;
	char		*polar_proxy_ssl_cipher_name;
	char		*polar_proxy_ssl_version;

	/* POLAR: Shared Server */
	uint32		polar_startup_gucs_hash;
} Port;

#ifdef USE_SSL
/*
 *	Hardcoded DH parameters, used in ephemeral DH keying.  (See also
 *	README.SSL for more details on EDH.)
 *
 *	If you want to create your own hardcoded DH parameters
 *	for fun and profit, review "Assigned Number for SKIP
 *	Protocols" (http://www.skip-vpn.org/spec/numbers.html)
 *	for suggestions.
 */
#define FILE_DH2048 \
"-----BEGIN DH PARAMETERS-----\n\
MIIBCAKCAQEA9kJXtwh/CBdyorrWqULzBej5UxE5T7bxbrlLOCDaAadWoxTpj0BV\n\
89AHxstDqZSt90xkhkn4DIO9ZekX1KHTUPj1WV/cdlJPPT2N286Z4VeSWc39uK50\n\
T8X8dryDxUcwYc58yWb/Ffm7/ZFexwGq01uejaClcjrUGvC/RgBYK+X0iP1YTknb\n\
zSC0neSRBzZrM2w4DUUdD3yIsxx8Wy2O9vPJI8BD8KVbGI2Ou1WMuF040zT9fBdX\n\
Q6MdGGzeMyEstSr/POGxKUAYEY18hKcKctaGxAMZyAcpesqVDNmWn6vQClCbAkbT\n\
CD1mpF1Bn5x8vYlLIhkmuquiXsNV6TILOwIBAg==\n\
-----END DH PARAMETERS-----\n"

/*
 * These functions are implemented by the glue code specific to each
 * SSL implementation (e.g. be-secure-openssl.c)
 */

/*
 * Initialize global SSL context.
 *
 * If isServerStart is true, report any errors as FATAL (so we don't return).
 * Otherwise, log errors at LOG level and return -1 to indicate trouble,
 * preserving the old SSL state if any.  Returns 0 if OK.
 */
extern int	be_tls_init(bool isServerStart);

/*
 * Destroy global SSL context, if any.
 */
extern void be_tls_destroy(void);

/*
 * Attempt to negotiate SSL connection.
 */
extern int	be_tls_open_server(Port *port);

/*
 * Close SSL connection.
 */
extern void be_tls_close(Port *port);

/*
 * Read data from a secure connection.
 */
extern ssize_t be_tls_read(Port *port, void *ptr, size_t len, int *waitfor);

/*
 * Write data to a secure connection.
 */
extern ssize_t be_tls_write(Port *port, void *ptr, size_t len, int *waitfor);

/*
 * Return information about the SSL connection.
 */
extern int	be_tls_get_cipher_bits(Port *port);
extern bool be_tls_get_compression(Port *port);
extern const char *be_tls_get_version(Port *port);
extern const char *be_tls_get_cipher(Port *port);
extern void be_tls_get_peerdn_name(Port *port, char *ptr, size_t len);

/*
 * Get the server certificate hash for SCRAM channel binding type
 * tls-server-end-point.
 *
 * The result is a palloc'd hash of the server certificate with its
 * size, and NULL if there is no certificate available.
 *
 * This is not supported with old versions of OpenSSL that don't have
 * the X509_get_signature_nid() function.
 */
#if defined(USE_OPENSSL) && defined(HAVE_X509_GET_SIGNATURE_NID)
#define HAVE_BE_TLS_GET_CERTIFICATE_HASH
extern char *be_tls_get_certificate_hash(Port *port, size_t *len);
#endif

#endif	/* USE_SSL */

extern ProtocolVersion FrontendProtocol;

/* TCP keepalives configuration. These are no-ops on an AF_UNIX socket. */

extern int	pq_getkeepalivesidle(Port *port);
extern int	pq_getkeepalivesinterval(Port *port);
extern int	pq_getkeepalivescount(Port *port);

extern int	pq_setkeepalivesidle(int idle, Port *port);
extern int	pq_setkeepalivesinterval(int interval, Port *port);
extern int	pq_setkeepalivescount(int count, Port *port);

#endif							/* LIBPQ_BE_H */
