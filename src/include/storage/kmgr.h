/*-------------------------------------------------------------------------
 *
 * kmgr.h
 *	  Key management module for transparent data encryption
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/storage/kmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_H
#define KMGR_H

#include "port/pg_crc32c.h"
#include "storage/relfilenode.h"
#include "storage/bufpage.h"

/*
 * Size of HMAC key for KEK is the same as the length of hash, we use
 * SHA-256.
 */
#define TDE_HMAC_KEY_SIZE		32

/* SHA-256 results 256 bits HMAC */
#define TDE_HMAC_SIZE			32

/* Size of key encryption key (KEK), which is always AES-256 key */
#define TDE_KEK_SIZE				32

/*
 * Max size of data encryption key. We support AES-128 and AES-256, the
 * maximum key size is 32.
 */
#define TDE_MAX_DEK_SIZE			32

/* Key wrapping appends the initial 8 bytes value */
#define TDE_DEK_WRAP_VALUE_SIZE		8

/* Wrapped key size is n+1 value */
#define TDE_MAX_WRAPPED_DEK_SIZE		(TDE_MAX_DEK_SIZE + TDE_DEK_WRAP_VALUE_SIZE)

#define TDE_MAX_PASSPHRASE_LEN		1024

/* Version identifier for kmgr file format like yyyymmddN */
#define KMGR_VERSION_NO	201912301

#define KMGR_PROMPT_MSG "Enter database encryption pass phrase:"

/* Kmgr file name */
#define KMGR_FILENAME "global/pg_kmgr"

/* data encryption key number, now the key contain rdek and wdek */
#define TDE_MAX_DEK 2

typedef unsigned char keydata_t;

/*
 * Struct for keys that needs to be verified using its HMAC.
 */
typedef struct WrappedEncKeyWithHmac
{
	keydata_t key[TDE_MAX_WRAPPED_DEK_SIZE];
	keydata_t hmac[TDE_HMAC_SIZE];
} WrappedEncKeyWithHmac;

/*
 * Struct for key manager meta data written in KMGR_FILENAME. Kmgr file stores
 * information that is used for TDE, verification and wrapped keys.
 * The file is written once when bootstrapping and is read when postmaster
 * startup.
 */
typedef struct KmgrFileData
{
	/* version for kmgr file */
	uint32		kmgr_version_no;

	/* Are data pages encrypted? Zero if encryption is disabled */
	uint32		data_encryption_cipher;

	/*
	 * Wrapped Key information for data encryption.
	 */
	WrappedEncKeyWithHmac tde_rdek;
	WrappedEncKeyWithHmac tde_wdek;

	/* CRC of all above ... MUST BE LAST! */
	pg_crc32c	crc;
} KmgrFileData;

/* GUC variable */
extern char *polar_cluster_passphrase_command;

extern void BootStrapKmgr(int bootstrap_data_encryption_cipher);
extern void InitializeKmgr(void);
extern const char *KmgrGetRelationEncryptionKey(void);
extern const char *KmgrGetWALEncryptionKey(void);
extern const char *KmgrGetKeyEncryptionKey(void);
extern int run_cluster_passphrase_command(const char *prompt,
										  char *buf, int size);
extern void get_kek_and_hmackey_from_passphrase(char *passphrase,
												Size passlen,
												keydata_t kek[TDE_KEK_SIZE],
												keydata_t hmackey[TDE_HMAC_KEY_SIZE]);
extern void write_kmgr_file(KmgrFileData *filedata);
extern KmgrFileData *polar_read_kmgr_file(void);
extern bool is_kmgr_file_exist(void);

#endif /* KMGR_H */
