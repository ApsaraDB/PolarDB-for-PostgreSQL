/*-----------------------------------------------------------------------
 *
 * PostgreSQL locale utilities for ICU
 *
 * Portions Copyright (c) 2002-2025, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/pg_locale_icu.c
 *
 *-----------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_ICU
#include <unicode/ucnv.h>
#include <unicode/ustring.h>

/*
 * ucol_strcollUTF8() was introduced in ICU 50, but it is buggy before ICU 53.
 * (see
 * <https://www.postgresql.org/message-id/flat/f1438ec6-22aa-4029-9a3b-26f79d330e72%40manitou-mail.org>)
 */
#if U_ICU_VERSION_MAJOR_NUM >= 53
#define HAVE_UCOL_STRCOLLUTF8 1
#else
#undef HAVE_UCOL_STRCOLLUTF8
#endif

#endif

#include "access/htup_details.h"
#include "catalog/pg_database.h"
#include "catalog/pg_collation.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/syscache.h"

/*
 * Size of stack buffer to use for string transformations, used to avoid heap
 * allocations in typical cases. This should be large enough that most strings
 * will fit, but small enough that we feel comfortable putting it on the
 * stack.
 */
#define		TEXTBUFLEN			1024

extern pg_locale_t create_pg_locale_icu(Oid collid, MemoryContext context);

#ifdef USE_ICU

extern UCollator *pg_ucol_open(const char *loc_str);

static size_t strlower_icu(char *dest, size_t destsize, const char *src,
						   ssize_t srclen, pg_locale_t locale);
static size_t strtitle_icu(char *dest, size_t destsize, const char *src,
						   ssize_t srclen, pg_locale_t locale);
static size_t strupper_icu(char *dest, size_t destsize, const char *src,
						   ssize_t srclen, pg_locale_t locale);
static size_t strfold_icu(char *dest, size_t destsize, const char *src,
						  ssize_t srclen, pg_locale_t locale);
static int	strncoll_icu(const char *arg1, ssize_t len1,
						 const char *arg2, ssize_t len2,
						 pg_locale_t locale);
static size_t strnxfrm_icu(char *dest, size_t destsize,
						   const char *src, ssize_t srclen,
						   pg_locale_t locale);
extern char *get_collation_actual_version_icu(const char *collcollate);

typedef int32_t (*ICU_Convert_Func) (UChar *dest, int32_t destCapacity,
									 const UChar *src, int32_t srcLength,
									 const char *locale,
									 UErrorCode *pErrorCode);

/*
 * Converter object for converting between ICU's UChar strings and C strings
 * in database encoding.  Since the database encoding doesn't change, we only
 * need one of these per session.
 */
static UConverter *icu_converter = NULL;

static UCollator *make_icu_collator(const char *iculocstr,
									const char *icurules);
static int	strncoll_icu(const char *arg1, ssize_t len1,
						 const char *arg2, ssize_t len2,
						 pg_locale_t locale);
static size_t strnxfrm_prefix_icu(char *dest, size_t destsize,
								  const char *src, ssize_t srclen,
								  pg_locale_t locale);
#ifdef HAVE_UCOL_STRCOLLUTF8
static int	strncoll_icu_utf8(const char *arg1, ssize_t len1,
							  const char *arg2, ssize_t len2,
							  pg_locale_t locale);
#endif
static size_t strnxfrm_prefix_icu_utf8(char *dest, size_t destsize,
									   const char *src, ssize_t srclen,
									   pg_locale_t locale);
static void init_icu_converter(void);
static size_t uchar_length(UConverter *converter,
						   const char *str, int32_t len);
static int32_t uchar_convert(UConverter *converter,
							 UChar *dest, int32_t destlen,
							 const char *src, int32_t srclen);
static int32_t icu_to_uchar(UChar **buff_uchar, const char *buff,
							size_t nbytes);
static size_t icu_from_uchar(char *dest, size_t destsize,
							 const UChar *buff_uchar, int32_t len_uchar);
static void icu_set_collation_attributes(UCollator *collator, const char *loc,
										 UErrorCode *status);
static int32_t icu_convert_case(ICU_Convert_Func func, pg_locale_t mylocale,
								UChar **buff_dest, UChar *buff_source,
								int32_t len_source);
static int32_t u_strToTitle_default_BI(UChar *dest, int32_t destCapacity,
									   const UChar *src, int32_t srcLength,
									   const char *locale,
									   UErrorCode *pErrorCode);
static int32_t u_strFoldCase_default(UChar *dest, int32_t destCapacity,
									 const UChar *src, int32_t srcLength,
									 const char *locale,
									 UErrorCode *pErrorCode);

static bool
char_is_cased_icu(char ch, pg_locale_t locale)
{
	return IS_HIGHBIT_SET(ch) ||
		(ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
}

/*
 * XXX: many of the functions below rely on casts directly from pg_wchar to
 * UChar32, which is correct for the UTF-8 encoding, but not in general.
 */

static pg_wchar
toupper_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_toupper(wc);
}

static pg_wchar
tolower_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_tolower(wc);
}

static const struct collate_methods collate_methods_icu = {
	.strncoll = strncoll_icu,
	.strnxfrm = strnxfrm_icu,
	.strnxfrm_prefix = strnxfrm_prefix_icu,
	.strxfrm_is_safe = true,
};

static const struct collate_methods collate_methods_icu_utf8 = {
#ifdef HAVE_UCOL_STRCOLLUTF8
	.strncoll = strncoll_icu_utf8,
#else
	.strncoll = strncoll_icu,
#endif
	.strnxfrm = strnxfrm_icu,
	.strnxfrm_prefix = strnxfrm_prefix_icu_utf8,
	.strxfrm_is_safe = true,
};

static bool
wc_isdigit_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_isdigit(wc);
}

static bool
wc_isalpha_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_isalpha(wc);
}

static bool
wc_isalnum_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_isalnum(wc);
}

static bool
wc_isupper_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_isupper(wc);
}

static bool
wc_islower_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_islower(wc);
}

static bool
wc_isgraph_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_isgraph(wc);
}

static bool
wc_isprint_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_isprint(wc);
}

static bool
wc_ispunct_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_ispunct(wc);
}

static bool
wc_isspace_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_isspace(wc);
}

static bool
wc_isxdigit_icu(pg_wchar wc, pg_locale_t locale)
{
	return u_isxdigit(wc);
}

static const struct ctype_methods ctype_methods_icu = {
	.strlower = strlower_icu,
	.strtitle = strtitle_icu,
	.strupper = strupper_icu,
	.strfold = strfold_icu,
	.wc_isdigit = wc_isdigit_icu,
	.wc_isalpha = wc_isalpha_icu,
	.wc_isalnum = wc_isalnum_icu,
	.wc_isupper = wc_isupper_icu,
	.wc_islower = wc_islower_icu,
	.wc_isgraph = wc_isgraph_icu,
	.wc_isprint = wc_isprint_icu,
	.wc_ispunct = wc_ispunct_icu,
	.wc_isspace = wc_isspace_icu,
	.wc_isxdigit = wc_isxdigit_icu,
	.char_is_cased = char_is_cased_icu,
	.wc_toupper = toupper_icu,
	.wc_tolower = tolower_icu,
};
#endif

pg_locale_t
create_pg_locale_icu(Oid collid, MemoryContext context)
{
#ifdef USE_ICU
	bool		deterministic;
	const char *iculocstr;
	const char *icurules = NULL;
	UCollator  *collator;
	pg_locale_t result;

	if (collid == DEFAULT_COLLATION_OID)
	{
		HeapTuple	tp;
		Datum		datum;
		bool		isnull;

		tp = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for database %u", MyDatabaseId);

		/* default database collation is always deterministic */
		deterministic = true;
		datum = SysCacheGetAttrNotNull(DATABASEOID, tp,
									   Anum_pg_database_datlocale);
		iculocstr = TextDatumGetCString(datum);
		datum = SysCacheGetAttr(DATABASEOID, tp,
								Anum_pg_database_daticurules, &isnull);
		if (!isnull)
			icurules = TextDatumGetCString(datum);

		ReleaseSysCache(tp);
	}
	else
	{
		Form_pg_collation collform;
		HeapTuple	tp;
		Datum		datum;
		bool		isnull;

		tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for collation %u", collid);
		collform = (Form_pg_collation) GETSTRUCT(tp);
		deterministic = collform->collisdeterministic;
		datum = SysCacheGetAttrNotNull(COLLOID, tp,
									   Anum_pg_collation_colllocale);
		iculocstr = TextDatumGetCString(datum);
		datum = SysCacheGetAttr(COLLOID, tp,
								Anum_pg_collation_collicurules, &isnull);
		if (!isnull)
			icurules = TextDatumGetCString(datum);

		ReleaseSysCache(tp);
	}

	collator = make_icu_collator(iculocstr, icurules);

	result = MemoryContextAllocZero(context, sizeof(struct pg_locale_struct));
	result->icu.locale = MemoryContextStrdup(context, iculocstr);
	result->icu.ucol = collator;
	result->deterministic = deterministic;
	result->collate_is_c = false;
	result->ctype_is_c = false;
	if (GetDatabaseEncoding() == PG_UTF8)
		result->collate = &collate_methods_icu_utf8;
	else
		result->collate = &collate_methods_icu;
	result->ctype = &ctype_methods_icu;

	return result;
#else
	/* could get here if a collation was created by a build with ICU */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("ICU is not supported in this build")));

	return NULL;
#endif
}

#ifdef USE_ICU

/*
 * Wrapper around ucol_open() to handle API differences for older ICU
 * versions.
 *
 * Ensure that no path leaks a UCollator.
 */
UCollator *
pg_ucol_open(const char *loc_str)
{
	UCollator  *collator;
	UErrorCode	status;
	const char *orig_str = loc_str;
	char	   *fixed_str = NULL;

	/*
	 * Must never open default collator, because it depends on the environment
	 * and may change at any time. Should not happen, but check here to catch
	 * bugs that might be hard to catch otherwise.
	 *
	 * NB: the default collator is not the same as the collator for the root
	 * locale. The root locale may be specified as the empty string, "und", or
	 * "root". The default collator is opened by passing NULL to ucol_open().
	 */
	if (loc_str == NULL)
		elog(ERROR, "opening default collator is not supported");

	/*
	 * In ICU versions 54 and earlier, "und" is not a recognized spelling of
	 * the root locale. If the first component of the locale is "und", replace
	 * with "root" before opening.
	 */
	if (U_ICU_VERSION_MAJOR_NUM < 55)
	{
		char		lang[ULOC_LANG_CAPACITY];

		status = U_ZERO_ERROR;
		uloc_getLanguage(loc_str, lang, ULOC_LANG_CAPACITY, &status);
		if (U_FAILURE(status) || status == U_STRING_NOT_TERMINATED_WARNING)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not get language from locale \"%s\": %s",
							loc_str, u_errorName(status))));
		}

		if (strcmp(lang, "und") == 0)
		{
			const char *remainder = loc_str + strlen("und");

			fixed_str = palloc(strlen("root") + strlen(remainder) + 1);
			strcpy(fixed_str, "root");
			strcat(fixed_str, remainder);

			loc_str = fixed_str;
		}
	}

	status = U_ZERO_ERROR;
	collator = ucol_open(loc_str, &status);
	if (U_FAILURE(status))
		ereport(ERROR,
		/* use original string for error report */
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not open collator for locale \"%s\": %s",
						orig_str, u_errorName(status))));

	if (U_ICU_VERSION_MAJOR_NUM < 54)
	{
		status = U_ZERO_ERROR;
		icu_set_collation_attributes(collator, loc_str, &status);

		/*
		 * Pretend the error came from ucol_open(), for consistent error
		 * message across ICU versions.
		 */
		if (U_FAILURE(status) || status == U_STRING_NOT_TERMINATED_WARNING)
		{
			ucol_close(collator);
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not open collator for locale \"%s\": %s",
							orig_str, u_errorName(status))));
		}
	}

	if (fixed_str != NULL)
		pfree(fixed_str);

	return collator;
}

/*
 * Create a UCollator with the given locale string and rules.
 *
 * Ensure that no path leaks a UCollator.
 */
static UCollator *
make_icu_collator(const char *iculocstr, const char *icurules)
{
	if (!icurules)
	{
		/* simple case without rules */
		return pg_ucol_open(iculocstr);
	}
	else
	{
		UCollator  *collator_std_rules;
		UCollator  *collator_all_rules;
		const UChar *std_rules;
		UChar	   *my_rules;
		UChar	   *all_rules;
		int32_t		length;
		int32_t		total;
		UErrorCode	status;

		/*
		 * If rules are specified, we extract the rules of the standard
		 * collation, add our own rules, and make a new collator with the
		 * combined rules.
		 */
		icu_to_uchar(&my_rules, icurules, strlen(icurules));

		collator_std_rules = pg_ucol_open(iculocstr);

		std_rules = ucol_getRules(collator_std_rules, &length);

		total = u_strlen(std_rules) + u_strlen(my_rules) + 1;

		/* avoid leaking collator on OOM */
		all_rules = palloc_extended(sizeof(UChar) * total, MCXT_ALLOC_NO_OOM);
		if (!all_rules)
		{
			ucol_close(collator_std_rules);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		}

		u_strcpy(all_rules, std_rules);
		u_strcat(all_rules, my_rules);

		ucol_close(collator_std_rules);

		status = U_ZERO_ERROR;
		collator_all_rules = ucol_openRules(all_rules, u_strlen(all_rules),
											UCOL_DEFAULT, UCOL_DEFAULT_STRENGTH,
											NULL, &status);
		if (U_FAILURE(status))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not open collator for locale \"%s\" with rules \"%s\": %s",
							iculocstr, icurules, u_errorName(status))));
		}

		return collator_all_rules;
	}
}

static size_t
strlower_icu(char *dest, size_t destsize, const char *src, ssize_t srclen,
			 pg_locale_t locale)
{
	int32_t		len_uchar;
	int32_t		len_conv;
	UChar	   *buff_uchar;
	UChar	   *buff_conv;
	size_t		result_len;

	len_uchar = icu_to_uchar(&buff_uchar, src, srclen);
	len_conv = icu_convert_case(u_strToLower, locale,
								&buff_conv, buff_uchar, len_uchar);
	result_len = icu_from_uchar(dest, destsize, buff_conv, len_conv);
	pfree(buff_uchar);
	pfree(buff_conv);

	return result_len;
}

static size_t
strtitle_icu(char *dest, size_t destsize, const char *src, ssize_t srclen,
			 pg_locale_t locale)
{
	int32_t		len_uchar;
	int32_t		len_conv;
	UChar	   *buff_uchar;
	UChar	   *buff_conv;
	size_t		result_len;

	len_uchar = icu_to_uchar(&buff_uchar, src, srclen);
	len_conv = icu_convert_case(u_strToTitle_default_BI, locale,
								&buff_conv, buff_uchar, len_uchar);
	result_len = icu_from_uchar(dest, destsize, buff_conv, len_conv);
	pfree(buff_uchar);
	pfree(buff_conv);

	return result_len;
}

static size_t
strupper_icu(char *dest, size_t destsize, const char *src, ssize_t srclen,
			 pg_locale_t locale)
{
	int32_t		len_uchar;
	int32_t		len_conv;
	UChar	   *buff_uchar;
	UChar	   *buff_conv;
	size_t		result_len;

	len_uchar = icu_to_uchar(&buff_uchar, src, srclen);
	len_conv = icu_convert_case(u_strToUpper, locale,
								&buff_conv, buff_uchar, len_uchar);
	result_len = icu_from_uchar(dest, destsize, buff_conv, len_conv);
	pfree(buff_uchar);
	pfree(buff_conv);

	return result_len;
}

static size_t
strfold_icu(char *dest, size_t destsize, const char *src, ssize_t srclen,
			pg_locale_t locale)
{
	int32_t		len_uchar;
	int32_t		len_conv;
	UChar	   *buff_uchar;
	UChar	   *buff_conv;
	size_t		result_len;

	len_uchar = icu_to_uchar(&buff_uchar, src, srclen);
	len_conv = icu_convert_case(u_strFoldCase_default, locale,
								&buff_conv, buff_uchar, len_uchar);
	result_len = icu_from_uchar(dest, destsize, buff_conv, len_conv);
	pfree(buff_uchar);
	pfree(buff_conv);

	return result_len;
}

/*
 * strncoll_icu_utf8
 *
 * Call ucol_strcollUTF8() or ucol_strcoll() as appropriate for the given
 * database encoding. An argument length of -1 means the string is
 * NUL-terminated.
 */
#ifdef HAVE_UCOL_STRCOLLUTF8
int
strncoll_icu_utf8(const char *arg1, ssize_t len1, const char *arg2, ssize_t len2,
				  pg_locale_t locale)
{
	int			result;
	UErrorCode	status;

	Assert(GetDatabaseEncoding() == PG_UTF8);

	status = U_ZERO_ERROR;
	result = ucol_strcollUTF8(locale->icu.ucol,
							  arg1, len1,
							  arg2, len2,
							  &status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("collation failed: %s", u_errorName(status))));

	return result;
}
#endif

/* 'srclen' of -1 means the strings are NUL-terminated */
size_t
strnxfrm_icu(char *dest, size_t destsize, const char *src, ssize_t srclen,
			 pg_locale_t locale)
{
	char		sbuf[TEXTBUFLEN];
	char	   *buf = sbuf;
	UChar	   *uchar;
	int32_t		ulen;
	size_t		uchar_bsize;
	Size		result_bsize;

	init_icu_converter();

	ulen = uchar_length(icu_converter, src, srclen);

	uchar_bsize = (ulen + 1) * sizeof(UChar);

	if (uchar_bsize > TEXTBUFLEN)
		buf = palloc(uchar_bsize);

	uchar = (UChar *) buf;

	ulen = uchar_convert(icu_converter, uchar, ulen + 1, src, srclen);

	result_bsize = ucol_getSortKey(locale->icu.ucol,
								   uchar, ulen,
								   (uint8_t *) dest, destsize);

	/*
	 * ucol_getSortKey() counts the nul-terminator in the result length, but
	 * this function should not.
	 */
	Assert(result_bsize > 0);
	result_bsize--;

	if (buf != sbuf)
		pfree(buf);

	/* if dest is defined, it should be nul-terminated */
	Assert(result_bsize >= destsize || dest[result_bsize] == '\0');

	return result_bsize;
}

/* 'srclen' of -1 means the strings are NUL-terminated */
size_t
strnxfrm_prefix_icu_utf8(char *dest, size_t destsize,
						 const char *src, ssize_t srclen,
						 pg_locale_t locale)
{
	size_t		result;
	UCharIterator iter;
	uint32_t	state[2];
	UErrorCode	status;

	Assert(GetDatabaseEncoding() == PG_UTF8);

	uiter_setUTF8(&iter, src, srclen);
	state[0] = state[1] = 0;	/* won't need that again */
	status = U_ZERO_ERROR;
	result = ucol_nextSortKeyPart(locale->icu.ucol,
								  &iter,
								  state,
								  (uint8_t *) dest,
								  destsize,
								  &status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("sort key generation failed: %s",
						u_errorName(status))));

	return result;
}

char *
get_collation_actual_version_icu(const char *collcollate)
{
	UCollator  *collator;
	UVersionInfo versioninfo;
	char		buf[U_MAX_VERSION_STRING_LENGTH];

	collator = pg_ucol_open(collcollate);

	ucol_getVersion(collator, versioninfo);
	ucol_close(collator);

	u_versionToString(versioninfo, buf);
	return pstrdup(buf);
}

/*
 * Convert a string in the database encoding into a string of UChars.
 *
 * The source string at buff is of length nbytes
 * (it needn't be nul-terminated)
 *
 * *buff_uchar receives a pointer to the palloc'd result string, and
 * the function's result is the number of UChars generated.
 *
 * The result string is nul-terminated, though most callers rely on the
 * result length instead.
 */
static int32_t
icu_to_uchar(UChar **buff_uchar, const char *buff, size_t nbytes)
{
	int32_t		len_uchar;

	init_icu_converter();

	len_uchar = uchar_length(icu_converter, buff, nbytes);

	*buff_uchar = palloc((len_uchar + 1) * sizeof(**buff_uchar));
	len_uchar = uchar_convert(icu_converter,
							  *buff_uchar, len_uchar + 1, buff, nbytes);

	return len_uchar;
}

/*
 * Convert a string of UChars into the database encoding.
 *
 * The source string at buff_uchar is of length len_uchar
 * (it needn't be nul-terminated)
 *
 * *result receives a pointer to the palloc'd result string, and the
 * function's result is the number of bytes generated (not counting nul).
 *
 * The result string is nul-terminated.
 */
static size_t
icu_from_uchar(char *dest, size_t destsize, const UChar *buff_uchar, int32_t len_uchar)
{
	UErrorCode	status;
	int32_t		len_result;

	init_icu_converter();

	status = U_ZERO_ERROR;
	len_result = ucnv_fromUChars(icu_converter, NULL, 0,
								 buff_uchar, len_uchar, &status);
	if (U_FAILURE(status) && status != U_BUFFER_OVERFLOW_ERROR)
		ereport(ERROR,
				(errmsg("%s failed: %s", "ucnv_fromUChars",
						u_errorName(status))));

	if (len_result + 1 > destsize)
		return len_result;

	status = U_ZERO_ERROR;
	len_result = ucnv_fromUChars(icu_converter, dest, len_result + 1,
								 buff_uchar, len_uchar, &status);
	if (U_FAILURE(status) ||
		status == U_STRING_NOT_TERMINATED_WARNING)
		ereport(ERROR,
				(errmsg("%s failed: %s", "ucnv_fromUChars",
						u_errorName(status))));

	return len_result;
}

static int32_t
icu_convert_case(ICU_Convert_Func func, pg_locale_t mylocale,
				 UChar **buff_dest, UChar *buff_source, int32_t len_source)
{
	UErrorCode	status;
	int32_t		len_dest;

	len_dest = len_source;		/* try first with same length */
	*buff_dest = palloc(len_dest * sizeof(**buff_dest));
	status = U_ZERO_ERROR;
	len_dest = func(*buff_dest, len_dest, buff_source, len_source,
					mylocale->icu.locale, &status);
	if (status == U_BUFFER_OVERFLOW_ERROR)
	{
		/* try again with adjusted length */
		pfree(*buff_dest);
		*buff_dest = palloc(len_dest * sizeof(**buff_dest));
		status = U_ZERO_ERROR;
		len_dest = func(*buff_dest, len_dest, buff_source, len_source,
						mylocale->icu.locale, &status);
	}
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("case conversion failed: %s", u_errorName(status))));
	return len_dest;
}

static int32_t
u_strToTitle_default_BI(UChar *dest, int32_t destCapacity,
						const UChar *src, int32_t srcLength,
						const char *locale,
						UErrorCode *pErrorCode)
{
	return u_strToTitle(dest, destCapacity, src, srcLength,
						NULL, locale, pErrorCode);
}

static int32_t
u_strFoldCase_default(UChar *dest, int32_t destCapacity,
					  const UChar *src, int32_t srcLength,
					  const char *locale,
					  UErrorCode *pErrorCode)
{
	uint32		options = U_FOLD_CASE_DEFAULT;
	char		lang[3];
	UErrorCode	status;

	/*
	 * Unlike the ICU APIs for lowercasing, titlecasing, and uppercasing, case
	 * folding does not accept a locale. Instead it just supports a single
	 * option relevant to Turkic languages 'az' and 'tr'; check for those
	 * languages to enable the option.
	 */
	status = U_ZERO_ERROR;
	uloc_getLanguage(locale, lang, 3, &status);
	if (U_SUCCESS(status))
	{
		/*
		 * The option name is confusing, but it causes u_strFoldCase to use
		 * the 'T' mappings, which are ignored for U_FOLD_CASE_DEFAULT.
		 */
		if (strcmp(lang, "tr") == 0 || strcmp(lang, "az") == 0)
			options = U_FOLD_CASE_EXCLUDE_SPECIAL_I;
	}

	return u_strFoldCase(dest, destCapacity, src, srcLength,
						 options, pErrorCode);
}

/*
 * strncoll_icu
 *
 * Convert the arguments from the database encoding to UChar strings, then
 * call ucol_strcoll(). An argument length of -1 means that the string is
 * NUL-terminated.
 *
 * When the database encoding is UTF-8, and ICU supports ucol_strcollUTF8(),
 * caller should call that instead.
 */
static int
strncoll_icu(const char *arg1, ssize_t len1,
			 const char *arg2, ssize_t len2, pg_locale_t locale)
{
	char		sbuf[TEXTBUFLEN];
	char	   *buf = sbuf;
	int32_t		ulen1;
	int32_t		ulen2;
	size_t		bufsize1;
	size_t		bufsize2;
	UChar	   *uchar1,
			   *uchar2;
	int			result;

	/* if encoding is UTF8, use more efficient strncoll_icu_utf8 */
#ifdef HAVE_UCOL_STRCOLLUTF8
	Assert(GetDatabaseEncoding() != PG_UTF8);
#endif

	init_icu_converter();

	ulen1 = uchar_length(icu_converter, arg1, len1);
	ulen2 = uchar_length(icu_converter, arg2, len2);

	bufsize1 = (ulen1 + 1) * sizeof(UChar);
	bufsize2 = (ulen2 + 1) * sizeof(UChar);

	if (bufsize1 + bufsize2 > TEXTBUFLEN)
		buf = palloc(bufsize1 + bufsize2);

	uchar1 = (UChar *) buf;
	uchar2 = (UChar *) (buf + bufsize1);

	ulen1 = uchar_convert(icu_converter, uchar1, ulen1 + 1, arg1, len1);
	ulen2 = uchar_convert(icu_converter, uchar2, ulen2 + 1, arg2, len2);

	result = ucol_strcoll(locale->icu.ucol,
						  uchar1, ulen1,
						  uchar2, ulen2);

	if (buf != sbuf)
		pfree(buf);

	return result;
}

/* 'srclen' of -1 means the strings are NUL-terminated */
static size_t
strnxfrm_prefix_icu(char *dest, size_t destsize,
					const char *src, ssize_t srclen,
					pg_locale_t locale)
{
	char		sbuf[TEXTBUFLEN];
	char	   *buf = sbuf;
	UCharIterator iter;
	uint32_t	state[2];
	UErrorCode	status;
	int32_t		ulen = -1;
	UChar	   *uchar = NULL;
	size_t		uchar_bsize;
	Size		result_bsize;

	/* if encoding is UTF8, use more efficient strnxfrm_prefix_icu_utf8 */
	Assert(GetDatabaseEncoding() != PG_UTF8);

	init_icu_converter();

	ulen = uchar_length(icu_converter, src, srclen);

	uchar_bsize = (ulen + 1) * sizeof(UChar);

	if (uchar_bsize > TEXTBUFLEN)
		buf = palloc(uchar_bsize);

	uchar = (UChar *) buf;

	ulen = uchar_convert(icu_converter, uchar, ulen + 1, src, srclen);

	uiter_setString(&iter, uchar, ulen);
	state[0] = state[1] = 0;	/* won't need that again */
	status = U_ZERO_ERROR;
	result_bsize = ucol_nextSortKeyPart(locale->icu.ucol,
										&iter,
										state,
										(uint8_t *) dest,
										destsize,
										&status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("sort key generation failed: %s",
						u_errorName(status))));

	return result_bsize;
}

static void
init_icu_converter(void)
{
	const char *icu_encoding_name;
	UErrorCode	status;
	UConverter *conv;

	if (icu_converter)
		return;					/* already done */

	icu_encoding_name = get_encoding_name_for_icu(GetDatabaseEncoding());
	if (!icu_encoding_name)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("encoding \"%s\" not supported by ICU",
						pg_encoding_to_char(GetDatabaseEncoding()))));

	status = U_ZERO_ERROR;
	conv = ucnv_open(icu_encoding_name, &status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("could not open ICU converter for encoding \"%s\": %s",
						icu_encoding_name, u_errorName(status))));

	icu_converter = conv;
}

/*
 * Find length, in UChars, of given string if converted to UChar string.
 *
 * A length of -1 indicates that the input string is NUL-terminated.
 */
static size_t
uchar_length(UConverter *converter, const char *str, int32_t len)
{
	UErrorCode	status = U_ZERO_ERROR;
	int32_t		ulen;

	ulen = ucnv_toUChars(converter, NULL, 0, str, len, &status);
	if (U_FAILURE(status) && status != U_BUFFER_OVERFLOW_ERROR)
		ereport(ERROR,
				(errmsg("%s failed: %s", "ucnv_toUChars", u_errorName(status))));
	return ulen;
}

/*
 * Convert the given source string into a UChar string, stored in dest, and
 * return the length (in UChars).
 *
 * A srclen of -1 indicates that the input string is NUL-terminated.
 */
static int32_t
uchar_convert(UConverter *converter, UChar *dest, int32_t destlen,
			  const char *src, int32_t srclen)
{
	UErrorCode	status = U_ZERO_ERROR;
	int32_t		ulen;

	status = U_ZERO_ERROR;
	ulen = ucnv_toUChars(converter, dest, destlen, src, srclen, &status);
	if (U_FAILURE(status))
		ereport(ERROR,
				(errmsg("%s failed: %s", "ucnv_toUChars", u_errorName(status))));
	return ulen;
}

/*
 * Parse collation attributes from the given locale string and apply them to
 * the open collator.
 *
 * First, the locale string is canonicalized to an ICU format locale ID such
 * as "und@colStrength=primary;colCaseLevel=yes". Then, it parses and applies
 * the key-value arguments.
 *
 * Starting with ICU version 54, the attributes are processed automatically by
 * ucol_open(), so this is only necessary for emulating this behavior on older
 * versions.
 */
pg_attribute_unused()
static void
icu_set_collation_attributes(UCollator *collator, const char *loc,
							 UErrorCode *status)
{
	int32_t		len;
	char	   *icu_locale_id;
	char	   *lower_str;
	char	   *str;
	char	   *token;

	/*
	 * The input locale may be a BCP 47 language tag, e.g.
	 * "und-u-kc-ks-level1", which expresses the same attributes in a
	 * different form. It will be converted to the equivalent ICU format
	 * locale ID, e.g. "und@colcaselevel=yes;colstrength=primary", by
	 * uloc_canonicalize().
	 */
	*status = U_ZERO_ERROR;
	len = uloc_canonicalize(loc, NULL, 0, status);
	icu_locale_id = palloc(len + 1);
	*status = U_ZERO_ERROR;
	len = uloc_canonicalize(loc, icu_locale_id, len + 1, status);
	if (U_FAILURE(*status) || *status == U_STRING_NOT_TERMINATED_WARNING)
		return;

	lower_str = asc_tolower(icu_locale_id, strlen(icu_locale_id));

	pfree(icu_locale_id);

	str = strchr(lower_str, '@');
	if (!str)
		return;
	str++;

	while ((token = strsep(&str, ";")))
	{
		char	   *e = strchr(token, '=');

		if (e)
		{
			char	   *name;
			char	   *value;
			UColAttribute uattr;
			UColAttributeValue uvalue;

			*status = U_ZERO_ERROR;

			*e = '\0';
			name = token;
			value = e + 1;

			/*
			 * See attribute name and value lists in ICU i18n/coll.cpp
			 */
			if (strcmp(name, "colstrength") == 0)
				uattr = UCOL_STRENGTH;
			else if (strcmp(name, "colbackwards") == 0)
				uattr = UCOL_FRENCH_COLLATION;
			else if (strcmp(name, "colcaselevel") == 0)
				uattr = UCOL_CASE_LEVEL;
			else if (strcmp(name, "colcasefirst") == 0)
				uattr = UCOL_CASE_FIRST;
			else if (strcmp(name, "colalternate") == 0)
				uattr = UCOL_ALTERNATE_HANDLING;
			else if (strcmp(name, "colnormalization") == 0)
				uattr = UCOL_NORMALIZATION_MODE;
			else if (strcmp(name, "colnumeric") == 0)
				uattr = UCOL_NUMERIC_COLLATION;
			else
				/* ignore if unknown */
				continue;

			if (strcmp(value, "primary") == 0)
				uvalue = UCOL_PRIMARY;
			else if (strcmp(value, "secondary") == 0)
				uvalue = UCOL_SECONDARY;
			else if (strcmp(value, "tertiary") == 0)
				uvalue = UCOL_TERTIARY;
			else if (strcmp(value, "quaternary") == 0)
				uvalue = UCOL_QUATERNARY;
			else if (strcmp(value, "identical") == 0)
				uvalue = UCOL_IDENTICAL;
			else if (strcmp(value, "no") == 0)
				uvalue = UCOL_OFF;
			else if (strcmp(value, "yes") == 0)
				uvalue = UCOL_ON;
			else if (strcmp(value, "shifted") == 0)
				uvalue = UCOL_SHIFTED;
			else if (strcmp(value, "non-ignorable") == 0)
				uvalue = UCOL_NON_IGNORABLE;
			else if (strcmp(value, "lower") == 0)
				uvalue = UCOL_LOWER_FIRST;
			else if (strcmp(value, "upper") == 0)
				uvalue = UCOL_UPPER_FIRST;
			else
			{
				*status = U_ILLEGAL_ARGUMENT_ERROR;
				break;
			}

			ucol_setAttribute(collator, uattr, uvalue, status);
		}
	}

	pfree(lower_str);
}

#endif							/* USE_ICU */
