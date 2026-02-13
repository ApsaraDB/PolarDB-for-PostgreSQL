--
-- Install the plperl and plperlu extensions
--

-- Before going ahead with the to-be-tested installations, verify that
-- a non-superuser is allowed to install plperl (but not plperlu) when
-- suitable permissions have been granted.

<<<<<<< HEAD
CREATE USER regress_user_plperl1;
CREATE USER regress_user_plperl2;

SET ROLE regress_user_plperl1;
=======
CREATE USER regress_plperl_user1;
CREATE USER regress_plperl_user2;

SET ROLE regress_plperl_user1;
>>>>>>> REL_15_16

CREATE EXTENSION plperl;  -- fail
CREATE EXTENSION plperlu;  -- fail

RESET ROLE;

DO $$
begin
<<<<<<< HEAD
  execute format('grant create on database %I to regress_user_plperl1',
=======
  execute format('grant create on database %I to regress_plperl_user1',
>>>>>>> REL_15_16
                 current_database());
end;
$$;

<<<<<<< HEAD
SET ROLE regress_user_plperl1;
=======
SET ROLE regress_plperl_user1;
>>>>>>> REL_15_16

CREATE EXTENSION plperl;
CREATE EXTENSION plperlu;  -- fail
CREATE SCHEMA plperl_setup_scratch;
SET search_path = plperl_setup_scratch;
<<<<<<< HEAD
GRANT ALL ON SCHEMA plperl_setup_scratch TO regress_user_plperl2;
=======
GRANT ALL ON SCHEMA plperl_setup_scratch TO regress_plperl_user2;
>>>>>>> REL_15_16

CREATE FUNCTION foo1() returns int language plperl as '1;';
SELECT foo1();

-- Must reconnect to avoid failure with non-MULTIPLICITY Perl interpreters
\c -
SET search_path = plperl_setup_scratch;

<<<<<<< HEAD
SET ROLE regress_user_plperl1;
=======
SET ROLE regress_plperl_user1;
>>>>>>> REL_15_16

-- Should be able to change privileges on the language
revoke all on language plperl from public;

<<<<<<< HEAD
SET ROLE regress_user_plperl2;

CREATE FUNCTION foo2() returns int language plperl as '2;';  -- fail

SET ROLE regress_user_plperl1;

grant usage on language plperl to regress_user_plperl2;

SET ROLE regress_user_plperl2;
=======
SET ROLE regress_plperl_user2;

CREATE FUNCTION foo2() returns int language plperl as '2;';  -- fail

SET ROLE regress_plperl_user1;

grant usage on language plperl to regress_plperl_user2;

SET ROLE regress_plperl_user2;
>>>>>>> REL_15_16

CREATE FUNCTION foo2() returns int language plperl as '2;';
SELECT foo2();

<<<<<<< HEAD
SET ROLE regress_user_plperl1;
=======
SET ROLE regress_plperl_user1;
>>>>>>> REL_15_16

-- Should be able to drop the extension, but not the language per se
DROP LANGUAGE plperl CASCADE;
DROP EXTENSION plperl CASCADE;

-- Clean up
RESET ROLE;
<<<<<<< HEAD
DROP OWNED BY regress_user_plperl1;
DROP USER regress_user_plperl1;
DROP USER regress_user_plperl2;
=======
DROP OWNED BY regress_plperl_user1;
DROP USER regress_plperl_user1;
DROP USER regress_plperl_user2;
>>>>>>> REL_15_16

-- Now install the versions that will be used by subsequent test scripts.
CREATE EXTENSION plperl;
CREATE EXTENSION plperlu;
