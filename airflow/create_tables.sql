CREATE TABLE IF NOT EXISTS public.staging_crypto (
	close_time datetime NOT NULL,
    open_price float,
    high_price float,
	low_price float,
	close_price float,
    volume float,
	quote_volume float,
    id int4 NOT NULL,
    symbol varchar(256),
    base_id int4 NOT NULL,
    base_symbol varchar(256),
    base_name varchar(256),
	base_fiat boolean,
	base_route varchar(256),
	quote_id int4 NOT NULL,
    quote_symbol varchar(256),
    quote_name varchar(256),
    quote_fiat boolean,
    quote_route varchar(256),
    "route" varchar(256),
    markets_id int8 NOT NULL,
	markets_exchange varchar(256),
	markets_pair varchar(256),
    markets_active boolean,
	markets_route varchar(256),
	close_date varchar(256)
);

CREATE TABLE IF NOT EXISTS public.staging_news (
	author varchar(65535),
	content varchar(65535),
	"description" varchar(65535),
	publishedAt datetime NOT NULL,
	source_id varchar(256) NOT NULL,
	source_name varchar(256),
	title varchar(65535) NOT NULL,
	"url" varchar(65535),
	urlToImage varchar(65535),
	published_date varchar(256),
	sentiment varchar(256),
	positive_score float,
	negative_score float,
	mixed_score float,
	neutral_score float
);

CREATE TABLE IF NOT EXISTS public.asset_base (
	id int4 NOT NULL,
	symbol varchar(256),
	name varchar(256),
	fiat boolean,
	"route" varchar(256),
	CONSTRAINT asset_base_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.asset_quote (
	id  int4 NOT NULL,
	symbol varchar(256),
	name varchar(256),
	fiat boolean,
	"route" varchar(256),
	CONSTRAINT asset_quote_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.asset_markets (
	id int4,
	exchange varchar(256),
	pair varchar(256),
	active boolean,
	"route" varchar(256),
	CONSTRAINT asset_markets_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public."time" (
	close_time timestamp NOT NULL,
	hour int4,
	day int4,
	week int4,
	month int4,
	year int4,
	weekday int4,
	CONSTRAINT close_time_pkey PRIMARY KEY (close_time)
);

CREATE TABLE IF NOT EXISTS public.articles (
	id bigint identity(0, 1),
	author varchar(65535),
	content varchar(65535),
	"description" varchar(65535),
	title varchar(65535),
	"url" varchar(65535),
	url_to_image varchar(65535),
	published_at datetime,
	published_date varchar(256),
	CONSTRAINT articles_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS public.sources (
	id varchar(256) NOT NULL,
	name varchar(256),
	CONSTRAINT sources_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.candlestick (
	id bigint identity(0, 1),
	ohlc_id int4,
	symbol varchar(256),
	base_id  int4,
	quote_id int4,
	"route" varchar(256),
	markets_id int4,
	close_date varchar(256),
	close_time timestamp,
	open_price float,
	high_price float,
	low_price float,
	close_price float,
	volume float,
	quote_volume float,
	article_id int8,
	source_id varchar(256),
	sentiment varchar(256),
	positive_score float,
	negative_score float,
	mixed_score float,
	neutral_score float,
	CONSTRAINT candlestick_pkey PRIMARY KEY (id)
);
