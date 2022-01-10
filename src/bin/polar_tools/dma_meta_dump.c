#include "polar_tools.h"
#include "polar_dma/consensus_log.h"

static struct option long_options[] = {
	{"pgdata", required_argument, NULL, 'D'},
	{"help", no_argument, NULL, '?'},
	{NULL, 0, NULL, 0}
};

static void
usage(void)
{
	printf("Dump dma meta usage:\n");
	printf(_(" {-D, --pgdata=}DATADIR  data directory\n"));
	printf("-?, --help show this help, then exit\n");
}

int 
dma_meta_main(int argc, char **argv)
{
	int	c;
	char file_path[MAXPGPATH];
	char *datadir= NULL;
	FILE *fp = NULL;
	bool succeed = false;
	ConsensusMetaHeader meta;
	pg_crc32c crc;
	char *member_info = NULL;
	char *learner_info = NULL;

	if (argc <= 1)
	{
		usage();
		return -1;
	}

	while ((c = getopt_long(argc, argv, "D:", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'D':
				datadir = optarg;
				break;

			default:
				usage();
				return -1;
		}
	}

	snprintf(file_path, MAXPGPATH, "%s/polar_dma/consensus_meta", datadir);

	fp = fopen(file_path, "r");
	if (fp == NULL)
	{
		fprintf(stderr, "Failed to open %s\n", file_path);
		goto end;
	}

	if (fread(&meta, 1, sizeof(ConsensusMetaHeader), fp) != sizeof(ConsensusMetaHeader))
	{
		fprintf(stderr, "Failed to read dma meta\n");
		goto end;
	}
	
	if (meta.version != DMA_META_VERSION)
		fprintf(stderr, "The version is unmatched , got %d, expect %d", meta.version, DMA_META_VERSION);

	if (meta.member_info_size > 0)
	{
		int member_info_size = meta.member_info_size;
		member_info = malloc(member_info_size);
		if (!member_info)
		{
			fprintf(stderr, "out of memory while load dma meta file");
			goto end;
		}

		if (fread(member_info, 1, member_info_size, fp) != member_info_size)
		{
			fprintf(stderr, "Failed to read member info for dma meta.\n");
			goto end;
		}
	}

	if (meta.learner_info_size > 0)
	{
		int learner_info_size = meta.learner_info_size;
		learner_info = malloc(learner_info_size);
		if (!learner_info)
		{
			fprintf(stderr, "out of memory while load dma meta file");
			goto end;
		}

		if (fread(learner_info, 1, learner_info_size, fp) != learner_info_size)
		{
			fprintf(stderr, "Failed to read learner info from dma meta.\n");
			goto end;
		}
	}

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, ((char *) &meta), offsetof(ConsensusMetaHeader, crc));
	if (meta.member_info_size > 0)
		COMP_CRC32C(crc, member_info, meta.member_info_size);
	if (meta.learner_info_size > 0)
		COMP_CRC32C(crc, learner_info, meta.learner_info_size);
	if (crc != meta.crc)
		fprintf(stderr, "The crc is incorrect, got %u, expect %u", crc, meta.crc);

	printf(_("dma_meta version:                  %d\n"), meta.version);
	printf(_("dma_meta current term:             %lu\n"), meta.current_term);
	printf(_("dma_meta current vote for:         %lu\n"), meta.vote_for);
	printf(_("dma_meta last leader term:         %lu\n"), meta.last_leader_term);
	printf(_("dma_meta last leader log index:    %lu\n"), meta.last_leader_log_index);
	printf(_("dma_meta scan index:               %lu\n"), meta.scan_index);
	printf(_("dma_meta cluster id:               %lu\n"), meta.cluster_id);
	printf(_("dma_meta commit index:             %lu\n"), meta.commit_index);
	printf(_("dma_meta purge index:              %lu\n"), meta.purge_index);
	printf(_("dma_meta member info size:         %u\n"), meta.member_info_size);
	printf(_("dma_meta learner info size:        %u\n"), meta.learner_info_size);

	if (meta.member_info_size > 0)
		printf(_("dma_meta member info:            %s\n"), member_info);
	if (meta.learner_info_size > 0)
		printf(_("dma_meta learner info:            %s\n"), learner_info);

	succeed = true;
end:
	if (fp)
		fclose(fp);
	if (member_info)
		free(member_info);
	if (learner_info)
		free(learner_info);

	return succeed ? 0 : -1;
}
