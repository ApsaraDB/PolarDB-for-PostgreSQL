select prosecdef from pg_proc where proname = 'get_indexes_to_reindex';

select prosecdef from pg_proc where pronamespace = 'polar_advisor'::regnamespace and prosecdef;
