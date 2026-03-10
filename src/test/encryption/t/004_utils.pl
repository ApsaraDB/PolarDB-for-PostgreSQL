use strict;
# 004_utils.pl
#
# Copyright (c) 2025, Alibaba Group Holding Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# IDENTIFICATION
#	  src/test/encryption/t/004_utils.pl

# 004_utils.pl
#
# Copyright (c) 2025, Alibaba Group Holding Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# IDENTIFICATION
#	  src/test/encryption/t/001_base.pl

use warnings;
use PostgreSQL::Test::Cluster;
use Test::More;

if ($ENV{with_tde} ne 'yes')
{
	plan skip_all => 'TDE not supported by this build';
}
else
{
	plan tests => 3;
}

my $keyword = "secret keyword";
my $node = PostgreSQL::Test::Cluster->new('test');
$node->init(
	extra => [
		'--cluster-passphrase-command',
		'echo "adfadsfadssssssssfa12312312312312312312312%p123"',
		'-e', 'aes-256',
	]);
$node->start;

# Test polar_tde_utils
$node->safe_psql('postgres', 'create extension polar_tde_utils');
my $ret = $node->safe_psql('postgres', 'select polar_tde_check_kmgr_file()');
is($ret, "t", 'Check tde kmgr file');
$ret = $node->safe_psql('postgres',
	'select kmgr_version_no from polar_tde_kmgr_info_view()');
is($ret, "201912301", 'Read kmgr version no');
$ret = $node->safe_psql('postgres',
	'select polar_tde_update_kmgr_file(\'echo "adfadsfadssssssssfa123123123123123123123123123123123123111313123"\')'
);
is($ret, "t", 'update top level key');
