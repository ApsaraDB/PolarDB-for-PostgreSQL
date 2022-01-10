/*
 * POLAR: only for test. Test if mock works properly.
 * This will be included into xlog.c if configure with --enable-polar-inject-tests.
 */
void
test_single_polar_xact_split_mock(char *guc_xids, int *xact_state_xids)
{
	TransactionState top_s = (TransactionState) palloc0(sizeof(TransactionStateData));
	int i = 0;

	polar_xact_split_xids = guc_xids;

	polar_xact_split_begin(top_s);

	Assert(xact_state_xids[0] == currentCommandId);
	Assert(xact_state_xids[1] == top_s->transactionId);
	for (i = 0; i < top_s->nChildXids; ++i)
		Assert(top_s->childXids[i] == xact_state_xids[i + 2]);

	polar_xact_split_end(top_s);

	Assert(top_s->childXids == NULL);
	Assert(top_s->nChildXids == 0);
	Assert(top_s->maxChildXids == 0);
	Assert(top_s->transactionId == InvalidTransactionId);
	Assert(currentCommandId == FirstCommandId);

	polar_xact_split_xids = NULL;

	pfree(top_s);
}
