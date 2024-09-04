---
author: digoal
date: 2023/02/03
minute: 25
---

# 生物、化学分子结构数据存储与计算、分析

<ArticleInfo :frontmatter=$frontmatter></ArticleInfo>

## 背景

PolarDB 的云原生存算分离架构, 具备低廉的数据存储、高效扩展弹性、高速多机并行计算能力、高速数据搜索和处理; PolarDB 与计算算法结合, 将实现双剑合璧, 推动业务数据的价值产出, 将数据变成生产力.

本文将介绍 PolarDB 开源版 通过 rdkit 支撑生物、化学分子结构数据存储与计算、分析

测试环境为 macOS+docker, PolarDB 部署请参考下文:

- [《如何用 PolarDB 证明巴菲特的投资理念 - 包括 PolarDB 简单部署》](https://github.com/digoal/blog/blob/master/202209/20220908_02.md)

## rdkit 介绍

分子具有连接、分形、图、组合的特征, 低级生命组成高级生命, 众多高级生命组成社会, 众多低维生物通过分形组成高维生物.

- [《用 PostgreSQL 递归 SQL 与 plpgsql 函数 绘制分形图 - 曼德勃罗集(Mandelbrot-上帝的指纹) 和 Julia 集 - `z->z^2+c`》](https://github.com/digoal/blog/blob/master/202208/20220818_02.md)

https://www.rdkit.org/docs/Overview.html

- Business-friendly BSD license
- Core data structures and algorithms in C++
- Python 3.x wrappers generated using Boost.Python
- Java and C# wrappers generated with SWIG
- 2D and 3D molecular operations
- Descriptor generation for machine learning
- Molecular database cartridge for PostgreSQL
- Cheminformatics nodes for KNIME (distributed from the KNIME community site: https://www.knime.com/rdkit)

PolarDB 通过 rdkit 插件实现生物、化学分子结构数据存储与计算、分析. (相似搜索、子结构或精确匹配搜索、分子比较等)

https://www.rdkit.org/docs/Cartridge.html

数据类型

```
postgres=#  \dT
        List of data types
 Schema |   Name    | Description
--------+-----------+-------------
 public | bfp       | a bit vector fingerprint
 public | _internal |
 public | mol       | an rdkit molecule.
 public | qmol      | an rdkit molecule containing query features
 public | reaction  |
 public | sfp       | a sparse count vector fingerprint
(6 rows)
```

操作符

```
postgres=# \do+
                                            List of operators
 Schema | Name | Left arg type | Right arg type |   Result type    |        Function        | Description
--------+------+---------------+----------------+------------------+------------------------+-------------
 public | #    | bfp           | bfp            | boolean          | public.dice_sml_op     |
 public | #    | sfp           | sfp            | boolean          | public.dice_sml_op     |
 public | %    | bfp           | bfp            | boolean          | public.tanimoto_sml_op |
 public | %    | sfp           | sfp            | boolean          | public.tanimoto_sml_op |
 public | <    | bfp           | bfp            | boolean          | bfp_lt                 |
 public | <    | mol           | mol            | boolean          | mol_lt                 |
 public | <    | sfp           | sfp            | boolean          | sfp_lt                 |
 public | <#>  | bfp           | bfp            | double precision | dice_dist              |
 public | <%>  | bfp           | bfp            | double precision | tanimoto_dist          |
 public | <=   | bfp           | bfp            | boolean          | bfp_le                 |
 public | <=   | mol           | mol            | boolean          | mol_le                 |
 public | <=   | sfp           | sfp            | boolean          | sfp_le                 |
 public | <>   | bfp           | bfp            | boolean          | bfp_ne                 |
 public | <>   | mol           | mol            | boolean          | mol_ne                 |
 public | <>   | reaction      | reaction       | boolean          | reaction_ne            |
 public | <>   | sfp           | sfp            | boolean          | sfp_ne                 |
 public | <@   | mol           | mol            | boolean          | public.rsubstruct      |
 public | <@   | qmol          | mol            | boolean          | public.rsubstruct      |
 public | <@   | reaction      | reaction       | boolean          | public.rsubstruct      |
 public | =    | bfp           | bfp            | boolean          | bfp_eq                 |
 public | =    | mol           | mol            | boolean          | mol_eq                 |
 public | =    | reaction      | reaction       | -                | -                      |
 public | =    | sfp           | sfp            | boolean          | sfp_eq                 |
 public | >    | bfp           | bfp            | boolean          | bfp_gt                 |
 public | >    | mol           | mol            | boolean          | mol_gt                 |
 public | >    | sfp           | sfp            | boolean          | sfp_gt                 |
 public | >=   | bfp           | bfp            | boolean          | bfp_ge                 |
 public | >=   | mol           | mol            | boolean          | mol_ge                 |
 public | >=   | sfp           | sfp            | boolean          | sfp_ge                 |
 public | ?<   | reaction      | reaction       | boolean          | rsubstructfp           |
 public | ?>   | reaction      | reaction       | boolean          | substructfp            |
 public | @=   | mol           | mol            | boolean          | mol_eq                 |
 public | @=   | reaction      | reaction       | boolean          | reaction_eq            |
 public | @>   | mol           | mol            | boolean          | public.substruct       |
 public | @>   | mol           | qmol           | boolean          | public.substruct       |
 public | @>   | reaction      | reaction       | boolean          | public.substruct       |
(36 rows)
```

函数

```
postgres=# \df
                                                                             List of functions
 Schema |             Name             | Result data type |                                           Argument data types                                            | Type
--------+------------------------------+------------------+----------------------------------------------------------------------------------------------------------+------
 public | add                          | sfp              | sfp, sfp                                                                                                 | func
 public | all_values_gt                | boolean          | sfp, integer                                                                                             | func
 public | all_values_lt                | boolean          | sfp, integer                                                                                             | func
 public | atompair_fp                  | sfp              | mol                                                                                                      | func
 public | atompairbv_fp                | bfp              | mol                                                                                                      | func
 public | avalon_fp                    | bfp              | mol, boolean DEFAULT false, integer DEFAULT 15761407                                                     | func
 public | bfp_cmp                      | integer          | bfp, bfp                                                                                                 | func
 public | bfp_eq                       | boolean          | bfp, bfp                                                                                                 | func
 public | bfp_from_binary_text         | bfp              | bytea                                                                                                    | func
 public | bfp_ge                       | boolean          | bfp, bfp                                                                                                 | func
 public | bfp_gt                       | boolean          | bfp, bfp                                                                                                 | func
 public | bfp_in                       | bfp              | cstring                                                                                                  | func
 public | bfp_le                       | boolean          | bfp, bfp                                                                                                 | func
 public | bfp_lt                       | boolean          | bfp, bfp                                                                                                 | func
 public | bfp_ne                       | boolean          | bfp, bfp                                                                                                 | func
 public | bfp_out                      | cstring          | bfp                                                                                                      | func
 public | bfp_to_binary_text           | bytea            | bfp                                                                                                      | func
 public | dice_dist                    | double precision | bfp, bfp                                                                                                 | func
 public | dice_sml                     | double precision | bfp, bfp                                                                                                 | func
 public | dice_sml                     | double precision | sfp, sfp                                                                                                 | func
 public | dice_sml_op                  | boolean          | bfp, bfp                                                                                                 | func
 public | dice_sml_op                  | boolean          | sfp, sfp                                                                                                 | func
 public | featmorgan_fp                | sfp              | mol, integer DEFAULT 2                                                                                   | func
 public | featmorganbv_fp              | bfp              | mol, integer DEFAULT 2                                                                                   | func
 public | fmcs                         | text             | mol                                                                                                      | agg
 public | fmcs                         | text             | text                                                                                                     | agg
 public | fmcs_mol_transition          | internal         | internal, mol                                                                                            | func
 public | fmcs_mols                    | text             | internal                                                                                                 | func
 public | fmcs_smiles                  | cstring          | cstring, cstring                                                                                         | func
 public | fmcs_smiles                  | text             | text                                                                                                     | func
 public | fmcs_smiles                  | text             | text, text                                                                                               | func
 public | fmcs_smiles_transition       | text             | text, text                                                                                               | func
 public | gbfp_compress                | internal         | internal                                                                                                 | func
 public | gbfp_consistent              | boolean          | internal, bytea, smallint, oid, internal                                                                 | func
 public | gbfp_decompress              | internal         | internal                                                                                                 | func
 public | gbfp_distance                | double precision | internal, bytea, smallint, oid                                                                           | func
 public | gbfp_fetch                   | internal         | internal                                                                                                 | func
 public | gbfp_penalty                 | internal         | internal, internal, internal                                                                             | func
 public | gbfp_picksplit               | internal         | internal, internal                                                                                       | func
 public | gbfp_same                    | internal         | internal, internal, internal                                                                             | func
 public | gbfp_union                   | _internal        | internal, internal                                                                                       | func
 public | gen_arr                      | integer[]        | normal integer, hot integer                                                                              | func
 public | gin_bfp_consistent           | boolean          | internal, smallint, bfp, integer, internal, internal, internal, internal                                 | func
 public | gin_bfp_extract_query        | internal         | bfp, internal, smallint, internal, internal, internal, internal                                          | func
 public | gin_bfp_extract_value        | internal         | bfp, internal                                                                                            | func
 public | gin_bfp_triconsistent        | boolean          | internal, smallint, bfp, integer, internal, internal, internal                                           | func
 public | gmol_compress                | internal         | internal                                                                                                 | func
 public | gmol_consistent              | boolean          | bytea, internal, integer                                                                                 | func
 public | gmol_decompress              | internal         | internal                                                                                                 | func
 public | gmol_penalty                 | internal         | internal, internal, internal                                                                             | func
 public | gmol_picksplit               | internal         | internal, internal                                                                                       | func
 public | gmol_same                    | internal         | bytea, bytea, internal                                                                                   | func
 public | gmol_union                   | integer[]        | bytea, internal                                                                                          | func
 public | greaction_compress           | internal         | internal                                                                                                 | func
 public | greaction_consistent         | boolean          | bytea, internal, integer                                                                                 | func
 public | gsfp_compress                | internal         | internal                                                                                                 | func
 public | gsfp_consistent              | boolean          | bytea, internal, integer                                                                                 | func
 public | gslfp_compress               | internal         | internal                                                                                                 | func
 public | gslfp_consistent             | boolean          | bytea, internal, integer                                                                                 | func
 public | gslfp_decompress             | internal         | internal                                                                                                 | func
 public | gslfp_penalty                | internal         | internal, internal, internal                                                                             | func
 public | gslfp_picksplit              | internal         | internal, internal                                                                                       | func
 public | gslfp_same                   | internal         | bytea, bytea, internal                                                                                   | func
 public | gslfp_union                  | integer[]        | bytea, internal                                                                                          | func
 public | has_reaction_substructmatch  | SETOF reaction   | queryreaction character, tablename regclass, columnname text                                             | func
 public | is_valid_ctab                | boolean          | cstring                                                                                                  | func
 public | is_valid_mol_pkl             | boolean          | bytea                                                                                                    | func
 public | is_valid_smarts              | boolean          | cstring                                                                                                  | func
 public | is_valid_smiles              | boolean          | cstring                                                                                                  | func
 public | layered_fp                   | bfp              | mol                                                                                                      | func
 public | maccs_fp                     | bfp              | mol                                                                                                      | func
 public | mol_adjust_query_properties  | mol              | mol, cstring DEFAULT ''::cstring                                                                         | func
 public | mol_adjust_query_properties  | qmol             | qmol, cstring DEFAULT ''::cstring                                                                        | func
 public | mol_amw                      | real             | mol                                                                                                      | func
 public | mol_chi0n                    | real             | mol                                                                                                      | func
 public | mol_chi0v                    | real             | mol                                                                                                      | func
 public | mol_chi1n                    | real             | mol                                                                                                      | func
 public | mol_chi1v                    | real             | mol                                                                                                      | func
 public | mol_chi2n                    | real             | mol                                                                                                      | func
 public | mol_chi2v                    | real             | mol                                                                                                      | func
 public | mol_chi3n                    | real             | mol                                                                                                      | func
 public | mol_chi3v                    | real             | mol                                                                                                      | func
 public | mol_chi4n                    | real             | mol                                                                                                      | func
 public | mol_chi4v                    | real             | mol                                                                                                      | func
 public | mol_cmp                      | integer          | mol, mol                                                                                                 | func
 public | mol_eq                       | boolean          | mol, mol                                                                                                 | func
 public | mol_exactmw                  | real             | mol                                                                                                      | func
 public | mol_formula                  | cstring          | mol, boolean DEFAULT false, boolean DEFAULT true                                                         | func
 public | mol_fractioncsp3             | real             | mol                                                                                                      | func
 public | mol_from_ctab                | mol              | cstring, boolean DEFAULT false                                                                           | func
 public | mol_from_json                | mol              | cstring                                                                                                  | func
 public | mol_from_pkl                 | mol              | bytea                                                                                                    | func
 public | mol_from_smiles              | mol              | cstring                                                                                                  | func
 public | mol_from_smiles              | mol              | text                                                                                                     | func
 public | mol_ge                       | boolean          | mol, mol                                                                                                 | func
 public | mol_gt                       | boolean          | mol, mol                                                                                                 | func
 public | mol_hallkieralpha            | real             | mol                                                                                                      | func
 public | mol_hba                      | integer          | mol                                                                                                      | func
 public | mol_hbd                      | integer          | mol                                                                                                      | func
 public | mol_in                       | mol              | cstring                                                                                                  | func
 public | mol_inchi                    | cstring          | mol, cstring DEFAULT ''::cstring                                                                         | func
 public | mol_inchikey                 | cstring          | mol, cstring DEFAULT ''::cstring                                                                         | func
 public | mol_kappa1                   | real             | mol                                                                                                      | func
 public | mol_kappa2                   | real             | mol                                                                                                      | func
 public | mol_kappa3                   | real             | mol                                                                                                      | func
 public | mol_labuteasa                | real             | mol                                                                                                      | func
 public | mol_le                       | boolean          | mol, mol                                                                                                 | func
 public | mol_logp                     | real             | mol                                                                                                      | func
 public | mol_lt                       | boolean          | mol, mol                                                                                                 | func
 public | mol_murckoscaffold           | mol              | mol                                                                                                      | func
 public | mol_ne                       | boolean          | mol, mol                                                                                                 | func
 public | mol_nm_hash                  | cstring          | mol, cstring DEFAULT 'AnonymousGraph'::cstring                                                           | func
 public | mol_numaliphaticcarbocycles  | integer          | mol                                                                                                      | func
 public | mol_numaliphaticheterocycles | integer          | mol                                                                                                      | func
 public | mol_numaliphaticrings        | integer          | mol                                                                                                      | func
 public | mol_numamidebonds            | integer          | mol                                                                                                      | func
 public | mol_numaromaticcarbocycles   | integer          | mol                                                                                                      | func
 public | mol_numaromaticheterocycles  | integer          | mol                                                                                                      | func
 public | mol_numaromaticrings         | integer          | mol                                                                                                      | func
 public | mol_numatoms                 | integer          | mol                                                                                                      | func
 public | mol_numbridgeheadatoms       | integer          | mol                                                                                                      | func
 public | mol_numheavyatoms            | integer          | mol                                                                                                      | func
 public | mol_numheteroatoms           | integer          | mol                                                                                                      | func
 public | mol_numheterocycles          | integer          | mol                                                                                                      | func
 public | mol_numrings                 | integer          | mol                                                                                                      | func
 public | mol_numrotatablebonds        | integer          | mol                                                                                                      | func
 public | mol_numsaturatedcarbocycles  | integer          | mol                                                                                                      | func
 public | mol_numsaturatedheterocycles | integer          | mol                                                                                                      | func
 public | mol_numsaturatedrings        | integer          | mol                                                                                                      | func
 public | mol_numspiroatoms            | integer          | mol                                                                                                      | func
 public | mol_out                      | cstring          | mol                                                                                                      | func
 public | mol_phi                      | real             | mol                                                                                                      | func
 public | mol_recv                     | mol              | internal                                                                                                 | func
 public | mol_send                     | bytea            | mol                                                                                                      | func
 public | mol_to_ctab                  | cstring          | mol, boolean DEFAULT true, boolean DEFAULT false                                                         | func
 public | mol_to_cxsmarts              | cstring          | mol                                                                                                      | func
 public | mol_to_cxsmarts              | cstring          | qmol                                                                                                     | func
 public | mol_to_cxsmiles              | cstring          | mol                                                                                                      | func
 public | mol_to_json                  | cstring          | mol                                                                                                      | func
 public | mol_to_json                  | cstring          | qmol                                                                                                     | func
 public | mol_to_pkl                   | bytea            | mol                                                                                                      | func
 public | mol_to_smarts                | cstring          | mol                                                                                                      | func
 public | mol_to_smarts                | cstring          | qmol                                                                                                     | func
 public | mol_to_smiles                | cstring          | mol                                                                                                      | func
 public | mol_to_smiles                | cstring          | qmol                                                                                                     | func
 public | mol_to_svg                   | cstring          | mol, cstring DEFAULT ''::cstring, integer DEFAULT 250, integer DEFAULT 200, cstring DEFAULT ''::cstring  | func
 public | mol_to_svg                   | cstring          | qmol, cstring DEFAULT ''::cstring, integer DEFAULT 250, integer DEFAULT 200, cstring DEFAULT ''::cstring | func
 public | mol_to_v3kctab               | cstring          | mol, boolean DEFAULT true                                                                                | func
 public | mol_tpsa                     | real             | mol                                                                                                      | func
 public | morgan_fp                    | sfp              | mol, integer DEFAULT 2                                                                                   | func
 public | morganbv_fp                  | bfp              | mol, integer DEFAULT 2                                                                                   | func
 public | qmol_from_ctab               | qmol             | cstring, boolean DEFAULT false                                                                           | func
 public | qmol_from_json               | qmol             | cstring                                                                                                  | func
 public | qmol_from_smarts             | qmol             | cstring                                                                                                  | func
 public | qmol_from_smiles             | qmol             | cstring                                                                                                  | func
 public | qmol_in                      | qmol             | cstring                                                                                                  | func
 public | qmol_out                     | cstring          | qmol                                                                                                     | func
 public | qmol_recv                    | qmol             | internal                                                                                                 | func
 public | qmol_send                    | bytea            | qmol                                                                                                     | func
 public | rdkit_fp                     | bfp              | mol                                                                                                      | func
 public | rdkit_toolkit_version        | text             |                                                                                                          | func
 public | rdkit_version                | text             |                                                                                                          | func
 public | reaction_difference_fp       | sfp              | reaction, integer DEFAULT 1                                                                              | func
 public | reaction_eq                  | boolean          | reaction, reaction                                                                                       | func
 public | reaction_from_ctab           | reaction         | cstring                                                                                                  | func
 public | reaction_from_smarts         | reaction         | cstring                                                                                                  | func
 public | reaction_from_smiles         | reaction         | cstring                                                                                                  | func
 public | reaction_in                  | reaction         | cstring                                                                                                  | func
 public | reaction_ne                  | boolean          | reaction, reaction                                                                                       | func
 public | reaction_numagents           | integer          | reaction                                                                                                 | func
 public | reaction_numproducts         | integer          | reaction                                                                                                 | func
 public | reaction_numreactants        | integer          | reaction                                                                                                 | func
 public | reaction_out                 | cstring          | reaction                                                                                                 | func
 public | reaction_recv                | reaction         | internal                                                                                                 | func
 public | reaction_send                | bytea            | reaction                                                                                                 | func
 public | reaction_structural_bfp      | bfp              | reaction, integer DEFAULT 5                                                                              | func
 public | reaction_to_ctab             | cstring          | reaction                                                                                                 | func
 public | reaction_to_smarts           | cstring          | reaction                                                                                                 | func
 public | reaction_to_smiles           | cstring          | reaction                                                                                                 | func
 public | reaction_to_svg              | cstring          | reaction, boolean DEFAULT false, integer DEFAULT 400, integer DEFAULT 200, cstring DEFAULT ''::cstring   | func
 public | rsubstruct                   | boolean          | mol, mol                                                                                                 | func
 public | rsubstruct                   | boolean          | qmol, mol                                                                                                | func
 public | rsubstruct                   | boolean          | reaction, reaction                                                                                       | func
 public | rsubstruct_chiral            | boolean          | mol, mol                                                                                                 | func
 public | rsubstructfp                 | boolean          | reaction, reaction                                                                                       | func
 public | sfp_cmp                      | integer          | sfp, sfp                                                                                                 | func
 public | sfp_eq                       | boolean          | sfp, sfp                                                                                                 | func
 public | sfp_ge                       | boolean          | sfp, sfp                                                                                                 | func
 public | sfp_gt                       | boolean          | sfp, sfp                                                                                                 | func
 public | sfp_in                       | sfp              | cstring                                                                                                  | func
 public | sfp_le                       | boolean          | sfp, sfp                                                                                                 | func
 public | sfp_lt                       | boolean          | sfp, sfp                                                                                                 | func
 public | sfp_ne                       | boolean          | sfp, sfp                                                                                                 | func
 public | sfp_out                      | cstring          | sfp                                                                                                      | func
 public | size                         | integer          | bfp                                                                                                      | func
 public | substruct                    | boolean          | mol, mol                                                                                                 | func
 public | substruct                    | boolean          | mol, qmol                                                                                                | func
 public | substruct                    | boolean          | reaction, reaction                                                                                       | func
 public | substruct_chiral             | boolean          | mol, mol                                                                                                 | func
 public | substruct_count              | integer          | mol, mol, boolean DEFAULT true                                                                           | func
 public | substruct_count              | integer          | mol, qmol, boolean DEFAULT true                                                                          | func
 public | substruct_count_chiral       | integer          | mol, mol, boolean DEFAULT true                                                                           | func
 public | substruct_count_chiral       | integer          | mol, qmol, boolean DEFAULT true                                                                          | func
 public | substructfp                  | boolean          | reaction, reaction                                                                                       | func
 public | subtract                     | sfp              | sfp, sfp                                                                                                 | func
 public | tanimoto_dist                | double precision | bfp, bfp                                                                                                 | func
 public | tanimoto_sml                 | double precision | bfp, bfp                                                                                                 | func
 public | tanimoto_sml                 | double precision | sfp, sfp                                                                                                 | func
 public | tanimoto_sml_op              | boolean          | bfp, bfp                                                                                                 | func
 public | tanimoto_sml_op              | boolean          | sfp, sfp                                                                                                 | func
 public | torsion_fp                   | sfp              | mol                                                                                                      | func
 public | torsionbv_fp                 | bfp              | mol                                                                                                      | func
 public | tversky_sml                  | double precision | bfp, bfp, real, real                                                                                     | func
(213 rows)
```

索引

```
 403 | btree_mol_ops
 403 | btree_bfp_ops
 403 | btree_sfp_ops
 405 | hash_mol_ops
 405 | hash_bfp_ops
 405 | hash_sfp_ops
 783 | gist_mol_ops
 783 | gist_qmol_ops
 783 | gist_bfp_ops
 783 | gist_sfp_ops
 783 | gist_sfp_low_ops
 783 | gist_reaction_ops
2742 | gin_bfp_ops
```

## 部署 rdkit on PolarDB

1、boost 依赖

```
wget https://boostorg.jfrog.io/artifactory/main/release/1.69.0/source/boost_1_69_0.tar.bz2

tar -jxvf boost_1_69_0.tar.bz2

cd boost_1_69_0

./bootstrap.sh --with-libraries=serialization

sudo ./b2 --prefix=/usr/local/boost -a install
```

2、cairo 依赖

```
sudo yum install -y cairo-devel cairo
```

3、freetype 依赖

```
wget https://download.savannah.gnu.org/releases/freetype/freetype-2.12.1.tar.gz
tar -zxvf freetype-2.12.1.tar.gz
cd freetype-2.12.1

./autogen.sh
./configure --prefix=/usr/local/freettype
make -j 6
sudo make install


sudo vi /etc/ld.so.conf
# add
/usr/local/freettype/lib

sudo ldconfig
```

4、rdkit

```
wget https://github.com/rdkit/rdkit/archive/refs/tags/Release_2022_09_3.tar.gz

tar -zxvf Release_2022_09_3.tar.gz
```

4\.1、Comic_Neue 依赖

```
## in macOS
https://fonts.google.com/download?family=Comic%20Neue

cp Comic_Neue.zip /home/postgres/rdkit-Release_2022_09_3/Code/GraphMol/MolDraw2D


## in docker
sudo chown postgres:postgres /home/postgres/rdkit-Release_2022_09_3/Code/GraphMol/MolDraw2D/Comic_Neue.zip
```

4\.2、rdkit

```
cd rdkit-Release_2022_09_3
mkdir build
cd build


cmake -DBOOST_ROOT=/usr/local/boost -DBoost_INCLUDE_DIR=/usr/local/boost/include -DRDK_BUILD_PYTHON_WRAPPERS=OFF -DRDK_BUILD_PGSQL=ON -DPostgreSQL_ROOT="/home/postgres/tmp_basedir_polardb_pg_1100_bld" -DFREETYPE_LIBRARY=/usr/local/freettype/lib/libfreetype.so.6 -DFREETYPE_INCLUDE_DIRS=/usr/local/freettype/include/freetype2 -DRDK_TEST_MULTITHREADED=OFF -DRDK_BUILD_INCHI_SUPPORT=ON -DRDK_BUILD_AVALON_SUPPORT=ON -DRDK_INSTALL_INTREE=OFF -DCMAKE_INSTALL_PREFIX=/usr/local/rdkit -Wno-dev ..

// OR

// cmake -DBOOST_ROOT=/usr/local/boost -DBoost_INCLUDE_DIR=/usr/local/boost/include -DRDK_BUILD_PYTHON_WRAPPERS=OFF -DRDK_BUILD_PGSQL=ON -DPostgreSQL_ROOT="/home/postgres/tmp_basedir_polardb_pg_1100_bld" -DFREETYPE_LIBRARY=/usr/local/freettype/lib/libfreetype.so.6 -DFREETYPE_INCLUDE_DIRS=/usr/local/freettype/include/freetype2 -DRDK_TEST_MULTITHREADED=OFF -DRDK_BUILD_INCHI_SUPPORT=ON -DRDK_BUILD_AVALON_SUPPORT=ON -DRDK_INSTALL_INTREE=OFF -DCMAKE_INSTALL_PREFIX=/usr/local/rdkit -DRDK_BUILD_MOLINTERCHANGE_SUPPORT=OFF -Wno-dev ..

// 编译时需要联网, cmake的时候需要git clone代码, 期间会下载几个依赖的软件, 如果没有下载成功就多试几次
// ...


make -j 6

// 编译时需要联网, make的时候也需要git clone代码

sudo make install
```

5、安装 rdkit 插件到 polardb.

```
psql
postgres=# create extension rdkit ;
CREATE EXTENSION
```

rdkit 编译选项:

```
rdkit-Release_2022_09_3/CMakeLists.txt

option(RDK_BUILD_SWIG_WRAPPERS "build the SWIG wrappers" OFF )
option(RDK_BUILD_PYTHON_WRAPPERS "build the standard python wrappers" ON )
option(RDK_BUILD_COMPRESSED_SUPPLIERS "build in support for compressed MolSuppliers" OFF )
option(RDK_BUILD_INCHI_SUPPORT "build the rdkit inchi wrapper" OFF )
option(RDK_BUILD_AVALON_SUPPORT "install support for the avalon toolkit. Use the variable AVALONTOOLS_DIR to set the location of the source." OFF )
option(RDK_BUILD_PGSQL "build the PostgreSQL cartridge" OFF )
option(RDK_BUILD_RPATH_SUPPORT "build shared libraries using rpath" OFF)
option(RDK_PGSQL_STATIC "statically link rdkit libraries into the PostgreSQL cartridge" ON )
option(RDK_BUILD_CONTRIB "build the Contrib directory" OFF )
option(RDK_INSTALL_INTREE "install the rdkit in the source tree (former behavior)" ON )
option(RDK_INSTALL_DLLS_MSVC "install the rdkit DLLs when using MSVC" OFF)
option(RDK_INSTALL_STATIC_LIBS "install the rdkit static libraries" ON )
option(RDK_INSTALL_PYTHON_TESTS "install the rdkit Python tests with the wrappers" OFF )
option(RDK_BUILD_THREADSAFE_SSS "enable thread-safe substructure searching" ON )
option(RDK_BUILD_SLN_SUPPORT "include support for the SLN format" ON )
option(RDK_TEST_MULTITHREADED "run some tests of multithreading" ON )
option(RDK_BUILD_SWIG_JAVA_WRAPPER "build the SWIG JAVA wrappers (does nothing if RDK_BUILD_SWIG_WRAPPERS is not set)" ON )
option(RDK_BUILD_SWIG_CSHARP_WRAPPER "build the experimental SWIG C# wrappers (does nothing if RDK_BUILD_SWIG_WRAPPERS is not set)" OFF )
option(RDK_SWIG_STATIC "statically link rdkit libraries into the SWIG wrappers" ON )
option(RDK_TEST_MMFF_COMPLIANCE "run MMFF compliance tests (requires tar/gzip)" ON )
option(RDK_BUILD_CPP_TESTS "build the c++ tests (disabing can speed up builds" ON)
option(RDK_USE_FLEXBISON "use flex/bison, if available, to build the SMILES/SMARTS/SLN parsers" OFF)
option(RDK_TEST_COVERAGE "Use G(L)COV to compute test coverage" OFF)
option(RDK_USE_BOOST_SERIALIZATION "Use the boost serialization library if available" ON)
option(RDK_USE_BOOST_STACKTRACE "use boost::stacktrace to do more verbose invariant output (linux only)" ON)
option(RDK_BUILD_TEST_GZIP "Build the gzip'd stream test" OFF)
option(RDK_OPTIMIZE_POPCNT "Use SSE4.2 popcount instruction while compiling." ON)
option(RDK_USE_STRICT_ROTOR_DEFINITION "Use the most strict rotatable bond definition" ON)
option(RDK_BUILD_DESCRIPTORS3D "Build the 3D descriptors calculators, requires Eigen3 to be installed" ON)
option(RDK_BUILD_FREESASA_SUPPORT "build the rdkit freesasa wrapper" OFF )
option(RDK_BUILD_COORDGEN_SUPPORT "build the rdkit coordgen wrapper" ON )
option(RDK_BUILD_MAEPARSER_SUPPORT "build the rdkit MAE parser wrapper" ON )
option(RDK_BUILD_MOLINTERCHANGE_SUPPORT "build in support for CommonChem molecule interchange" ON )
option(RDK_BUILD_YAEHMOP_SUPPORT "build support for the YAeHMOP wrapper" OFF)
option(RDK_BUILD_XYZ2MOL_SUPPORT "build in support for the RDKit's implementation of xyz2mol (in the DetermineBonds library)" OFF )
option(RDK_BUILD_STRUCTCHECKER_SUPPORT "build in support for the StructChecker alpha (not recommended, use the MolVS integration instead)" OFF )
option(RDK_USE_URF "Build support for Florian Flachsenberg's URF library" ON)
option(RDK_INSTALL_DEV_COMPONENT "install libraries and headers" ON)
option(RDK_USE_BOOST_REGEX "use boost::regex instead of std::regex (needed for systems with g++-4.8)" OFF)
option(RDK_USE_BOOST_IOSTREAMS "use boost::iostreams" ON)
option(RDK_BUILD_MINIMAL_LIB "build the minimal RDKit wrapper (for the JS bindings)" OFF)
option(RDK_BUILD_CFFI_LIB "build the CFFI wrapper (for use in other programming languges)" OFF)
option(RDK_BUILD_FUZZ_TARGETS "build the fuzz targets" OFF)
```

make installcheck

```
cd rdkit-Release_2022_09_3/Code/PgSQL/rdkit

[postgres@aa25c5be9681 rdkit]$ USE_PGXS=1 make installcheck
/home/postgres/tmp_basedir_polardb_pg_1100_bld/lib/pgxs/src/makefiles/../../src/test/regress/pg_regress --inputdir=./ --bindir='/home/postgres/tmp_basedir_polardb_pg_1100_bld/bin'      --dbname=contrib_regression rdkit-91 props btree molgist bfpgist-91 bfpgin sfpgist slfpgist fps reaction
(using postmaster on 127.0.0.1, default port)
============== dropping database "contrib_regression" ==============
DROP DATABASE
============== creating database "contrib_regression" ==============
CREATE DATABASE
ALTER DATABASE
============== running regression test queries        ==============
test rdkit-91                     ... ok
test props                        ... ok
test btree                        ... ok
test molgist                      ... ok
test bfpgist-91                   ... ok
test bfpgin                       ... ok
test sfpgist                      ... ok
test slfpgist                     ... ok
test fps                          ... ok
test reaction                     ... ok


===========================================================
 All 10 tests passed.

 POLARDB:
 All 10 tests, 0 tests in ignore, 0 tests in polar ignore.
===========================================================
```

## 参考

- [《重新发现 PostgreSQL 之美 - 35 茅山道士 - rdkit 化学分析》](https://github.com/digoal/blog/blob/master/202106/20210624_01.md)
- [《PostgreSQL 化学插件 - pgchem_tigress molecules rdkit》](https://github.com/digoal/blog/blob/master/202003/20200326_06.md)
- [《PostgreSQL 化学分析 - RDKit Cartridge 1 - 环境部署》](https://github.com/digoal/blog/blob/master/201911/20191125_01.md)
- https://www.rdkit.org/docs/Cartridge.html
