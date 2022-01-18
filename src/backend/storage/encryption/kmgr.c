/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	 Encryption key management module.
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 *
 * IDENTIFICATION
 *	  src/backend/storage/encryption/kmgr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/xlog.h"
#include "common/sha2.h"
#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/kmgr.h"
#include "storage/polar_fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "utils/syscache.h"

/*
 * Key encryption key. This variable is set during verification
 * of user given passphrase. After verified, the plain key data
 * is set to this variable.
 */
static keydata_t keyEncKey[TDE_KEK_SIZE];

/*
 * Relation encryption key and WAL encryption key.  Similar to
 * key encryption key, these variables store the plain key data.
 */
static keydata_t relEncKey[TDE_MAX_DEK_SIZE];
static keydata_t walEncKey[TDE_MAX_DEK_SIZE];

static char *tde_key_types[TDE_MAX_DEK] ={
		"relation",
		"wal"
};

/* GUC variable */
char *polar_cluster_passphrase_command = NULL;

static bool verify_passphrase(char *passphrase, int passlen,
							  WrappedEncKeyWithHmac *rdek,
							  WrappedEncKeyWithHmac *wdek);
static void generate_key_and_hmac(keydata_t *key, keydata_t *hmackey,
		const char *type, keydata_t *enc_key, keydata_t *hmac);
/*
 * This func must be called ONCE on system install. we derive KEK,
 * generate MDEK and salt, compute hmac, write kmgr file etc.
 */
void
BootStrapKmgr(int bootstrap_data_encryption_cipher)
{
	KmgrFileData *kmgrfile;
	char passphrase[TDE_MAX_PASSPHRASE_LEN];
	keydata_t hmackey[TDE_HMAC_KEY_SIZE];
	keydata_t *rdek_enc;
	keydata_t *wdek_enc;
	keydata_t *rdek_hmac;
	keydata_t *wdek_hmac;
	int	len;

	if (bootstrap_data_encryption_cipher == TDE_ENCRYPTION_OFF)
		return;

#ifndef USE_OPENSSL
	ereport(ERROR,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 (errmsg("cluster encryption is not supported because OpenSSL is not supported by this build"),
			  errhint("Compile with --with-openssl to use cluster encryption."))));
#endif

	/* Fill out the kmgr file contents */
	kmgrfile = palloc0(sizeof(KmgrFileData));
	kmgrfile->data_encryption_cipher = bootstrap_data_encryption_cipher;
	rdek_enc = kmgrfile->tde_rdek.key;
	rdek_hmac = kmgrfile->tde_rdek.hmac;
	wdek_enc = kmgrfile->tde_wdek.key;
	wdek_hmac = kmgrfile->tde_wdek.hmac;

	/*
	 * Set data encryption cipher so that subsequent bootstrapping process
	 * can proceed.
	 */
	SetConfigOption("polar_data_encryption_cipher",
					EncryptionCipherString(bootstrap_data_encryption_cipher),
					PGC_INTERNAL, PGC_S_OVERRIDE);

	 /* Get encryption key passphrase */
	len = run_cluster_passphrase_command(KMGR_PROMPT_MSG,
										 passphrase,
										 TDE_MAX_PASSPHRASE_LEN);

	/* Get key encryption key and HMAC key from passphrase */
	get_kek_and_hmackey_from_passphrase(passphrase, len, keyEncKey, hmackey);

	/*
	 * Generate relation encryption key and WAL encryption key.
	 * The generated two keys must be stored in relEncKey and
	 * walEncKey that can be used by other modules since even
	 * during bootstrapping we need to encrypt both systemcatalogs
	 * and WAL.
	 */

	generate_key_and_hmac(relEncKey, hmackey, tde_key_types[0], rdek_enc, rdek_hmac);
	generate_key_and_hmac(walEncKey, hmackey, tde_key_types[1], wdek_enc, wdek_hmac);

	/* write kmgr file to the disk */
	write_kmgr_file(kmgrfile);
	pfree(kmgrfile);
}

/*
 * Run cluster_passphrase_command
 *
 * prompt will be substituted for %p.
 *
 * The result will be put in buffer buf, which is of size size.	 The return
 * value is the length of the actual result.
 */
int
run_cluster_passphrase_command(const char *prompt, char *buf, int size)
{
	StringInfoData command;
	char	   *p;
	FILE	   *fh;
	int			pclose_rc;
	size_t		len = 0;

	Assert(prompt);
	Assert(size > 0);
	buf[0] = '\0';

	initStringInfo(&command);

	for (p = polar_cluster_passphrase_command; *p; p++)
	{
		if (p[0] == '%')
		{
			switch (p[1])
			{
				case 'p':
					appendStringInfoString(&command, prompt);
					p++;
					break;
				case '%':
					appendStringInfoChar(&command, '%');
					p++;
					break;
				default:
					appendStringInfoChar(&command, p[0]);
			}
		}
		else
			appendStringInfoChar(&command, p[0]);
	}



	fh = OpenPipeStream(command.data, "r");
	if (fh == NULL)
	{
		/*no cover begin*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\": %m",
						command.data)));
		goto error;
		/*no cover end*/
	}

	if (!fgets(buf, size, fh))
	{
		if (ferror(fh))
		{
			/*no cover begin*/
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from command \"%s\": %m",
							command.data)));
			goto error;
			/*no cover end*/
		}
	}

	pclose_rc = ClosePipeStream(fh);
	if (pclose_rc == -1)
	{
		/*no cover begin*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close pipe to external command: %m")));
		goto error;
		/*no cover end*/
	}
	else if (pclose_rc != 0)
	{
		/*no cover begin*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("command \"%s\" failed",
						command.data),
				 errdetail_internal("%s", wait_result_to_str(pclose_rc))));
		goto error;
		/*no cover end*/
	}

	/* strip trailing newline */
	len = strlen(buf);
	if (len > 0 && buf[len - 1] == '\n')
		buf[--len] = '\0';

error:
	pfree(command.data);
	return len;
}

/*
 * Get encryption key passphrase and verify it, then get the un-encrypted
 * RDEK and WDEK. This function is called by postmaster at startup time.
 */
void
InitializeKmgr(void)
{
	KmgrFileData *kmgrfile;
	WrappedEncKeyWithHmac *wrapped_rdek;
	WrappedEncKeyWithHmac *wrapped_wdek;
	char passphrase[TDE_MAX_PASSPHRASE_LEN];
	int		len;
	int		wrapped_keysize;
	int		unwrapped_size;

	if (!is_kmgr_file_exist())
		return;

	/* Get contents of kmgr file */
	kmgrfile = polar_read_kmgr_file();

	/* Get cluster passphrase */
	len = run_cluster_passphrase_command(KMGR_PROMPT_MSG, passphrase, TDE_MAX_PASSPHRASE_LEN);

	/* Get two wrapped keys stored in kmgr file */
	wrapped_rdek = &(kmgrfile->tde_rdek);
	wrapped_wdek = &(kmgrfile->tde_wdek);

	wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;

	/* Verify the correctness of given passphrase */
	if (!verify_passphrase(passphrase, len, wrapped_rdek, wrapped_wdek))
	{
		/*no cover begin*/
		pfree(kmgrfile);
		ereport(ERROR,
				(errmsg("cluster passphrase does not match expected passphrase")));
		/*no cover end*/
	}

	/* The passphrase is correct, unwrap both RDEK and WDEK */
	pg_unwrap_key(keyEncKey, TDE_KEK_SIZE,
				  wrapped_rdek->key, wrapped_keysize,
				  relEncKey, &unwrapped_size);
	if (unwrapped_size != EncryptionKeySize)
	{
		/*no cover begin*/
		pfree(kmgrfile);
		elog(ERROR, "unwrapped relation encryption key size is invalid, got %d expected %d",
			 unwrapped_size, EncryptionKeySize);
		/*no cover end*/
	}

	pg_unwrap_key(keyEncKey, TDE_KEK_SIZE,
				  wrapped_wdek->key, wrapped_keysize,
				  walEncKey, &unwrapped_size);
	pfree(kmgrfile);
	if (unwrapped_size != EncryptionKeySize)
	{
		/*no cover begin*/
		elog(ERROR, "unwrapped WAL encryptoin key size is invalid, got %d expected %d",
			 unwrapped_size, EncryptionKeySize);
		/*no cover end*/
	}
}

/*
 * Hash the given passphrase and extract it into KEK and HMAC
 * key.
 */
void
get_kek_and_hmackey_from_passphrase(char *passphrase, Size passlen,
									keydata_t kek_out[TDE_KEK_SIZE],
									keydata_t hmackey_out[TDE_HMAC_KEY_SIZE])
{
	keydata_t enckey_and_hmackey[PG_SHA512_DIGEST_LENGTH];
	pg_sha512_ctx ctx;

	pg_sha512_init(&ctx);
	pg_sha512_update(&ctx, (const uint8 *) passphrase, passlen);
	pg_sha512_final(&ctx, enckey_and_hmackey);

	/*
	 * SHA-512 results 64 bytes. We extract it into two keys for
	 * each 32 bytes: one for key encryption and another one for
	 * HMAC.
	 */
	memcpy(kek_out, enckey_and_hmackey, TDE_KEK_SIZE);
	memcpy(hmackey_out, enckey_and_hmackey + TDE_KEK_SIZE, TDE_HMAC_KEY_SIZE);
}

/*
 * Verify the correctness of the given passphrase. We compute HMACs of the
 * wrapped keys (RDEK and WDEK) using the HMAC key retrived from the user
 * provided passphrase. And then we compare it with the HMAC stored alongside
 * the controlfile. Return true if both HMACs are matched, meaning the given
 * passphrase is correct. Otherwise return false.
 */
static bool
verify_passphrase(char *passphrase, int passlen,
				  WrappedEncKeyWithHmac *rdek, WrappedEncKeyWithHmac *wdek)
{
	keydata_t user_kek[TDE_KEK_SIZE];
	keydata_t user_hmackey[TDE_HMAC_KEY_SIZE];
	keydata_t result_hmac[TDE_HMAC_SIZE];
	int	wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;

	get_kek_and_hmackey_from_passphrase(passphrase, passlen,
										user_kek, user_hmackey);

	/* Verify both HMACs of RDEK and WDEK */
	pg_compute_hmac(user_hmackey, TDE_HMAC_KEY_SIZE,
					rdek->key, wrapped_keysize,
					result_hmac);
	if (memcmp(result_hmac, rdek->hmac, TDE_HMAC_SIZE) != 0)
		return false;

	pg_compute_hmac(user_hmackey, TDE_HMAC_KEY_SIZE,
					wdek->key, wrapped_keysize,
					result_hmac);
	if (memcmp(result_hmac, wdek->hmac, TDE_HMAC_SIZE) != 0)
		return false;

	/* The passphrase is verified. Save the key encryption key */
	memcpy(keyEncKey, user_kek, TDE_KEK_SIZE);

	return true;
}

/* Return plain relation encryption key */
const char *
KmgrGetRelationEncryptionKey(void)
{
	Assert(DataEncryptionEnabled());
	return (const char *) relEncKey;
}

/* Return plain WAL encryption key */
/*no cover begin*/
const char *
KmgrGetWALEncryptionKey(void)
{
	Assert(DataEncryptionEnabled());
	return (const char *) walEncKey;
}
/*no cover end*/

/* Return plain key encryption key */
const char *
KmgrGetKeyEncryptionKey(void)
{
	Assert(DataEncryptionEnabled());
	return (const char *) keyEncKey;
}

/*
 * Read kmgr file, and return palloc'd file data.
 */
KmgrFileData *
polar_read_kmgr_file(void)
{
	KmgrFileData *kmgrfile;
	pg_crc32c	crc;
	int read_len;
	int fd;
	char			polar_kmgr_file[MAXPGPATH];

	/*
	 * When POLARDB read it in the postmaster,
	 * shared storage has not been mounted in the plugin,
	 * so please assert it.
	 */
	Assert((polar_enable_shared_storage_mode &&
			polar_vfs_switch == POLAR_VFS_SWITCH_PLUGIN)
			|| (!polar_enable_shared_storage_mode));

	polar_make_file_path_level2(polar_kmgr_file, KMGR_FILENAME);

	fd = BasicOpenFile(polar_kmgr_file, O_RDONLY | PG_BINARY, true);

	if (fd < 0)
		/*no cover begin*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", KMGR_FILENAME)));
		/*no cover end*/

	/* Read data */
	kmgrfile = (KmgrFileData *) palloc(sizeof(KmgrFileData));

	pgstat_report_wait_start(WAIT_EVENT_KMGR_FILE_READ);
	if ((read_len = polar_read(fd, kmgrfile, sizeof(KmgrFileData))) != sizeof(KmgrFileData))
	{
		/*no cover begin*/
		polar_close(fd);
		ereport(ERROR,
					(errcode_for_file_access(),
					 (errmsg("could not read from file \"%s\": %m", KMGR_FILENAME))));
		/*no cover end*/
	}
	pgstat_report_wait_end();

	if (polar_close(fd))
		/*no cover begin*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", KMGR_FILENAME)));
		/*no cover end*/

	/* Verify CRC */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (char *) kmgrfile, offsetof(KmgrFileData, crc));
	FIN_CRC32C(crc);

	if (!EQ_CRC32C(crc, kmgrfile->crc))
		/*no cover line*/
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("calculated CRC checksum does not match value stored in file \"%s\"",
						KMGR_FILENAME)));


	if (polar_enable_shared_storage_mode)
		elog(LOG, "polardb load kmgr file success");

	SetConfigOption("polar_data_encryption_cipher",
					EncryptionCipherString(kmgrfile->data_encryption_cipher),
					PGC_INTERNAL, PGC_S_OVERRIDE);

	return kmgrfile;
}

/*
 * Write kmgr file. This function is used only when bootstrapping.
 * NB: In the initdb, we will write it to local, and move to POLARSTORE with polar-initdb.sh.
 */
void
write_kmgr_file(KmgrFileData *filedata)
{
	int				fd;
	char			polar_kmgr_file[MAXPGPATH];

	filedata->kmgr_version_no = KMGR_VERSION_NO;
	INIT_CRC32C(filedata->crc);
	COMP_CRC32C(filedata->crc, filedata, offsetof(KmgrFileData, crc));
	FIN_CRC32C(filedata->crc);

	polar_make_file_path_level2(polar_kmgr_file, KMGR_FILENAME);

	fd = BasicOpenFile(polar_kmgr_file, PG_BINARY | O_CREAT | O_RDWR, true);

	if (fd < 0)
	{
		/*no cover line*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						KMGR_FILENAME)));
		return;
	}

	pgstat_report_wait_start(WAIT_EVENT_KMGR_FILE_WRITE);
	if (polar_write(fd, filedata, sizeof(KmgrFileData)) != sizeof(KmgrFileData))
		/*no cover line*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write kmgr file \"%s\": %m",
						KMGR_FILENAME)));
	pgstat_report_wait_end();

	pgstat_report_wait_start(WAIT_EVENT_KMGR_FILE_SYNC);
	if (polar_fsync(fd) != 0)
		/*no cover line*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 (errmsg("could not sync file \"%s\": %m",
						 KMGR_FILENAME))));
	pgstat_report_wait_end();

	if (polar_close(fd))
		/*no cover line*/
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", KMGR_FILENAME)));
}

bool
is_kmgr_file_exist(void)
{
	char			polar_kmgr_file[MAXPGPATH];
	struct stat st;

	polar_make_file_path_level2(polar_kmgr_file, KMGR_FILENAME);

	if (polar_stat(polar_kmgr_file, &st) == 0)
		return true;
	else
		return false;
}

static void
generate_key_and_hmac(keydata_t *key, keydata_t *hmackey,
		const char *type, keydata_t *enc_key, keydata_t *hmac)
{
	int size;
	int	wrapped_keysize;
	if (!pg_strong_random(key, EncryptionKeySize))
		/*no cover line*/
		ereport(ERROR,
				(errmsg("failed to generate %s encryption key", type)));

	/* Wrap key by KEK */
	wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;
	pg_wrap_key(keyEncKey, TDE_KEK_SIZE,
				key, EncryptionKeySize,
				enc_key, &size);
	if (size != wrapped_keysize)
		/*no cover line*/
		elog(ERROR, "wrapped %s encryption key size is invalid, got %d expected %d",
			 type, size, wrapped_keysize);
	pg_compute_hmac(hmackey, TDE_HMAC_KEY_SIZE,
					enc_key, wrapped_keysize,
					hmac);
}
