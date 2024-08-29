CREATE FUNCTION jieba_start(internal, integer)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION jieba_query_start(internal, integer)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION jieba_gettoken(internal, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION jieba_gettoken_with_position(internal, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION jieba_end(internal)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION jieba_lextype(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION jieba_load_user_dict(integer)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE type word_type as enum('nz');

CREATE TABLE jieba_user_dict (
	word     text,
	dict_name int   not null default 0,
	weight   float not null default 10,
	type     word_type not null default 'nz',
	primary key(dict_name, word)
);

CREATE TEXT SEARCH PARSER jieba (
	START    = jieba_start,
	GETTOKEN = jieba_gettoken,
	END      = jieba_end,
	LEXTYPES = jieba_lextype,
	HEADLINE = pg_catalog.prsd_headline
);

CREATE TEXT SEARCH PARSER jiebaqry (
	START    = jieba_query_start,
	GETTOKEN = jieba_gettoken,
	END      = jieba_end,
	LEXTYPES = jieba_lextype,
	HEADLINE = pg_catalog.prsd_headline
);

CREATE TEXT SEARCH PARSER jieba_position (
	START    = jieba_start,
	GETTOKEN = jieba_gettoken_with_position,
	END      = jieba_end,
	LEXTYPES = jieba_lextype,
	HEADLINE = pg_catalog.prsd_headline
);

CREATE TEXT SEARCH CONFIGURATION jiebacfg (PARSER = jieba);
COMMENT ON TEXT SEARCH CONFIGURATION jiebacfg IS 'Mix segmentation configuration for jieba';

CREATE TEXT SEARCH CONFIGURATION jiebaqry (PARSER = jiebaqry);
COMMENT ON TEXT SEARCH CONFIGURATION jiebaqry IS 'Query segmentation configuration for jieba';

CREATE TEXT SEARCH CONFIGURATION jiebacfg_pos (PARSER = jieba_position);
COMMENT ON TEXT SEARCH CONFIGURATION jiebacfg_pos IS 'Mix segmentation configuration for jieba(with position)';

CREATE TEXT SEARCH DICTIONARY jieba_stem (TEMPLATE=simple, stopwords = 'jieba');
COMMENT ON TEXT SEARCH DICTIONARY jieba_stem IS 'jieba dictionary: lower case and check for stopword which including Unicode symbols that are mainly Chinese characters and punctuations';

ALTER TEXT SEARCH CONFIGURATION jiebacfg ADD MAPPING FOR nz,n,m,i,l,d,s,t,mq,nr,j,a,r,b,f,nrt,v,z,ns,q,vn,c,nt,u,o,zg,nrfg,df,p,g,y,ad,vg,ng,x,ul,k,ag,dg,rr,rg,an,vq,e,uv,tg,mg,ud,vi,vd,uj,uz,h,ug,rz WITH jieba_stem;
ALTER TEXT SEARCH CONFIGURATION jiebaqry ADD MAPPING FOR nz,n,m,i,l,d,s,t,mq,nr,j,a,r,b,f,nrt,v,z,ns,q,vn,c,nt,u,o,zg,nrfg,df,p,g,y,ad,vg,ng,x,ul,k,ag,dg,rr,rg,an,vq,e,uv,tg,mg,ud,vi,vd,uj,uz,h,ug,rz WITH jieba_stem;
ALTER TEXT SEARCH CONFIGURATION jiebacfg_pos ADD MAPPING FOR nz,n,m,i,l,d,s,t,mq,nr,j,a,r,b,f,nrt,v,z,ns,q,vn,c,nt,u,o,zg,nrfg,df,p,g,y,ad,vg,ng,x,ul,k,ag,dg,rr,rg,an,vq,e,uv,tg,mg,ud,vi,vd,uj,uz,h,ug,rz WITH jieba_stem;
