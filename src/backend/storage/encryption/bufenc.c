/*-------------------------------------------------------------------------
 *
 * bufenc.c
 *
 * Portions Copyright (c) 2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/encryption/bufenc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/bufpage.h"
#include "storage/encryption.h"
#include "storage/fd.h"
#include "storage/kmgr.h"
#include "utils/guc.h"

static char buf_encryption_iv[ENC_IV_SIZE];

static void set_buffer_encryption_iv(Page page, BlockNumber blocknum);

void
EncryptBufferBlock(BlockNumber blocknum, Page page)
{
	PageSetEncrypted(page);
	set_buffer_encryption_iv(page, blocknum);
	pg_tde_encrypt(page + PageEncryptOffset,
			   page + PageEncryptOffset,
			   SizeOfPageEncryption,
			   KmgrGetRelationEncryptionKey(),
			   buf_encryption_iv);
	if (polar_enable_debug)
	{
		/*no cover line*/
		elog(DEBUG1, "POLARDB: Page %u is encrypted, enc offset %lu, encsize %lu, iv is %s, key is %s",
				blocknum, PageEncryptOffset, SizeOfPageEncryption,
				buf_encryption_iv, KmgrGetRelationEncryptionKey());
	}
}
void
DecryptBufferBlock(BlockNumber blocknum, Page page)
{
	set_buffer_encryption_iv(page, blocknum);
	pg_decrypt(page + PageEncryptOffset,
			   page + PageEncryptOffset,
			   SizeOfPageEncryption,
			   KmgrGetRelationEncryptionKey(),
			   buf_encryption_iv);
	if (polar_enable_debug)
	{
		/*no cover line*/
		elog(DEBUG1, "POLARDB: Page %u is decrypted.", blocknum);
	}
}

static void
set_buffer_encryption_iv(Page page, BlockNumber blocknum)
{
	char *p = buf_encryption_iv;

	MemSet(buf_encryption_iv, 0, ENC_IV_SIZE);

	/* page lsn (8 byte) */
	memcpy(p, &((PageHeader) page)->pd_lsn, sizeof(PageXLogRecPtr));
	p += sizeof(PageXLogRecPtr);

	/* block number (4 byte) */
	memcpy(p, &blocknum, sizeof(BlockNumber));
	p += sizeof(BlockNumber);

	/* Space for counter (4 byte) */
	MemSet(p, 0, ENC_BUFFER_AES_COUNTER_SIZE);
	p += ENC_BUFFER_AES_COUNTER_SIZE;
}

