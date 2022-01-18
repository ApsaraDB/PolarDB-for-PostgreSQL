/*-------------------------------------------------------------------------
 *
 * polar_tde_utils.c
 *
 *
 * IDENTIFICATION
 *	  external/polar_tde_utils/polar_tde_utils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "storage/enc_common.h"
#include "storage/kmgr.h"
#include "storage/enc_cipher.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"

//POLAR: declaration for heap_form_tuple
#include "access/htup_details.h"

PG_MODULE_MAGIC;


Datum polar_tde_update_kmgr_file(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(polar_tde_update_kmgr_file);

Datum polar_tde_kmgr_info_view(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(polar_tde_kmgr_info_view);

Datum polar_tde_check_kmgr_file(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(polar_tde_check_kmgr_file);

Datum
polar_tde_update_kmgr_file(PG_FUNCTION_ARGS)
{
	char	*polar_cluster_passphrase_command_new = text_to_cstring(PG_GETARG_TEXT_PP(0));
	KmgrFileData *kmgrfile;
	keydata_t key_enc_key_new[TDE_KEK_SIZE];
	char passphrase[TDE_MAX_PASSPHRASE_LEN];
	keydata_t hmackey[TDE_HMAC_KEY_SIZE];
	keydata_t *rdek_enc;
	keydata_t *wdek_enc;
	keydata_t *rdek_hmac;
	keydata_t *wdek_hmac;
	int	wrapped_keysize;
	int	len;
	int size;

        if (!superuser())
                ereport(ERROR,
                                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                errmsg("must be superuser to execute polar_tde_update_kmgr_file.")));

	if (data_encryption_cipher == TDE_ENCRYPTION_OFF){
		elog(ERROR, "data_encryption_cipher is off, can not update kmgr file.");
		PG_RETURN_BOOL(false);
	}

#ifndef USE_OPENSSL
	elog(ERROR, "Cluster encryption is not supported because OpenSSL is not supported by this build, "
			"compile with --with-openssl to use cluster encryption.");
	PG_RETURN_BOOL(false);
#endif

	/* Update the polar_cluster_passphrase_command so we can use the new one */
	SetConfigOption("polar_cluster_passphrase_command",
					polar_cluster_passphrase_command_new,
					PGC_SIGHUP, PGC_S_SESSION);

	/* Fill out the kmgr file contents */
	kmgrfile = palloc0(sizeof(KmgrFileData));
	kmgrfile->data_encryption_cipher = data_encryption_cipher;
	rdek_enc = kmgrfile->tde_rdek.key;
	rdek_hmac = kmgrfile->tde_rdek.hmac;
	wdek_enc = kmgrfile->tde_wdek.key;
	wdek_hmac = kmgrfile->tde_wdek.hmac;

	 /* Get encryption key passphrase */
	len = run_cluster_passphrase_command(KMGR_PROMPT_MSG,
										 passphrase,
										 TDE_MAX_PASSPHRASE_LEN);

	/* Get key encryption key and HMAC key from passphrase */
	get_kek_and_hmackey_from_passphrase(passphrase, len, key_enc_key_new, hmackey);

	/* Get relation encryption key and wal encryption key from memory */

	/* Wrap both keys by KEK */
	wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;
	pg_wrap_key(key_enc_key_new, TDE_KEK_SIZE,
				(unsigned char *) KmgrGetRelationEncryptionKey(), EncryptionKeySize,
				rdek_enc, &size);
	if (size != wrapped_keysize){
		elog(ERROR, "wrapped relation encryption key size is invalid, got %d expected %d",
			 size, wrapped_keysize);
		PG_RETURN_BOOL(false);
	}

	pg_wrap_key(key_enc_key_new, TDE_KEK_SIZE,
				(unsigned char *) KmgrGetWALEncryptionKey(), EncryptionKeySize,
				wdek_enc, &size);
	if (size != wrapped_keysize){
		elog(ERROR, "wrapped WAL encryption key size is invalid, got %d expected %d",
			 size, wrapped_keysize);
		PG_RETURN_BOOL(false);
	}

	/* Compute both HMAC */
	pg_compute_hmac(hmackey, TDE_HMAC_KEY_SIZE,
					rdek_enc, wrapped_keysize,
					rdek_hmac);
	pg_compute_hmac(hmackey, TDE_HMAC_KEY_SIZE,
					wdek_enc, wrapped_keysize,
					wdek_hmac);

	/* write kmgr file to the disk, the kmgr file is very small, so we don't use rename.*/
	write_kmgr_file(kmgrfile);
	pfree(kmgrfile);
	PG_RETURN_BOOL(true);
}

Datum
polar_tde_check_kmgr_file(PG_FUNCTION_ARGS)
{
	KmgrFileData *kmgrfile;
	WrappedEncKeyWithHmac *wrapped_rdek;
	WrappedEncKeyWithHmac *wrapped_wdek;
	char passphrase[TDE_MAX_PASSPHRASE_LEN];

	keydata_t user_kek[TDE_KEK_SIZE];
	keydata_t user_hmackey[TDE_HMAC_KEY_SIZE];
	keydata_t result_hmac[TDE_HMAC_SIZE];

	keydata_t rel_enc_key_in_file[TDE_MAX_DEK_SIZE];
	keydata_t wal_enc_key_in_file[TDE_MAX_DEK_SIZE];

	int		len;
	int		wrapped_keysize;
	int		unwrapped_size;

	bool	kmgr_file_is_right = true;

        if (!superuser())
                ereport(ERROR,
                                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                 errmsg("must be superuser to execute polar_tde_check_kmgr_file.")));

	if (!is_kmgr_file_exist()){
		elog(WARNING, "Can not find the kmgr file, check kmgr file failed!");
		kmgr_file_is_right = false;
	}

	/* Get contents of kmgr file */
	kmgrfile = polar_read_kmgr_file();

	if (kmgrfile->kmgr_version_no != KMGR_VERSION_NO){
		elog(WARNING, "kmgr_version_no in the kmgr file is not right!");
		kmgr_file_is_right = false;
	}

	if (kmgrfile->data_encryption_cipher != data_encryption_cipher){
		elog(WARNING, "data_encryption_cipher in the kmgr file is not right!");
		kmgr_file_is_right = false;
	}

	/* Get cluster passphrase */
	len = run_cluster_passphrase_command(KMGR_PROMPT_MSG, passphrase, TDE_MAX_PASSPHRASE_LEN);

	/* Get two wrapped keys stored in kmgr file */
	wrapped_rdek = &(kmgrfile->tde_rdek);
	wrapped_wdek = &(kmgrfile->tde_wdek);

	wrapped_keysize = EncryptionKeySize + TDE_DEK_WRAP_VALUE_SIZE;

	get_kek_and_hmackey_from_passphrase(passphrase, len,
											user_kek, user_hmackey);

	/* Verify both HMACs of RDEK and WDEK */
	pg_compute_hmac(user_hmackey, TDE_HMAC_KEY_SIZE,
					wrapped_rdek->key, wrapped_keysize,
					result_hmac);
	if (memcmp(result_hmac, wrapped_rdek->hmac, TDE_HMAC_SIZE) != 0){
		elog(WARNING, "the hmac of the rel encryption key is not the same as kmgr file."
				"the hmac of the rel encryption key is %s, "
				"the hmac of the rel encryption key from kmgr file is %s",
				result_hmac, wrapped_rdek->hmac);
		kmgr_file_is_right = false;
	}

	pg_compute_hmac(user_hmackey, TDE_HMAC_KEY_SIZE,
					wrapped_wdek->key, wrapped_keysize,
					result_hmac);
	if (memcmp(result_hmac, wrapped_wdek->hmac, TDE_HMAC_SIZE) != 0){
		elog(WARNING, "the hmac of the wal encryption key is not the same as kmgr file."
				"the hmac of the wal encryption key is %s, "
				"the hmac of the wal encryption key from kmgr file is %s",
				result_hmac, wrapped_wdek->hmac);
		kmgr_file_is_right = false;
	}

	/* The passphrase is correct, unwrap both RDEK and WDEK */
	pg_unwrap_key(user_kek, TDE_KEK_SIZE,
				  wrapped_rdek->key, wrapped_keysize,
				  rel_enc_key_in_file, &unwrapped_size);
	if (memcmp(rel_enc_key_in_file, KmgrGetRelationEncryptionKey(), EncryptionKeySize) != 0){
		elog(WARNING, "rel encryption key is not the same as the kmgr file: "
				"rel_enckey in memory is %s, rel_enckey in file is %s",
				KmgrGetRelationEncryptionKey(), rel_enc_key_in_file);
		kmgr_file_is_right = false;
	}

	pg_unwrap_key(user_kek, TDE_KEK_SIZE,
				  wrapped_wdek->key, wrapped_keysize,
				  wal_enc_key_in_file, &unwrapped_size);
	if (memcmp(wal_enc_key_in_file, KmgrGetWALEncryptionKey(), EncryptionKeySize) != 0){
		elog(WARNING, "wal encryption key is not the same as the kmgr file: "
				"wal_enckey in memory is %s, rel_enckey in file is %s",
				KmgrGetRelationEncryptionKey(), wal_enc_key_in_file);
		kmgr_file_is_right = false;
	}
	pfree(kmgrfile);
	PG_RETURN_BOOL(kmgr_file_is_right);
}

Datum
polar_tde_kmgr_info_view(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext oldcontext;
	HeapTuple	tuple;
	Datum		values[5];
	bool		isnull[5];
	KmgrFileData *kmgrfile;

	char rel_enc_key_hex[TDE_MAX_DEK_SIZE * 2 + 1];
	char wal_enc_key_hex[TDE_MAX_DEK_SIZE * 2 + 1];
	char key_enc_key_hex[TDE_KEK_SIZE * 2 + 1];

        if (!superuser())
                ereport(ERROR,
                                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                                 errmsg("must be superuser to execute polar_tde_kmgr_info_view.")));

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = NULL;
	rsinfo->setDesc = NULL;

	tupdesc = CreateTemplateTupleDesc(5, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "kmgr_version_no",
						INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "data_encryption_cipher",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "rdek_key_hex",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "wdek_key_hex",
						TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "kek_enckey_hex",
						TEXTOID, -1, 0);

	oldcontext = MemoryContextSwitchTo(
							rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	memset(isnull, false, sizeof(isnull));
	memset(values, 0, sizeof(values));
	kmgrfile = polar_read_kmgr_file();
	hex_encode(KmgrGetRelationEncryptionKey(), TDE_MAX_DEK_SIZE, rel_enc_key_hex);
	rel_enc_key_hex[TDE_MAX_DEK_SIZE * 2] = '\0';
	hex_encode(KmgrGetWALEncryptionKey(), TDE_MAX_DEK_SIZE, wal_enc_key_hex);
	wal_enc_key_hex[TDE_MAX_DEK_SIZE * 2] = '\0';
	hex_encode(KmgrGetKeyEncryptionKey(), TDE_KEK_SIZE, key_enc_key_hex);
	key_enc_key_hex[TDE_MAX_DEK_SIZE * 2] = '\0';

	values[0] = Int32GetDatum(kmgrfile->kmgr_version_no);
	values[1] = CStringGetTextDatum(EncryptionCipherString(kmgrfile->data_encryption_cipher));
	values[2] = CStringGetTextDatum(rel_enc_key_hex);
	values[3] = CStringGetTextDatum(wal_enc_key_hex);
	values[4] = CStringGetTextDatum(key_enc_key_hex);
	tuple = heap_form_tuple(tupdesc, values, isnull);
	tuplestore_puttuple(tupstore, tuple);
	tuplestore_donestoring(tupstore);
	pfree(kmgrfile);
	return (Datum) 0;
}


