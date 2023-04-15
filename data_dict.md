# Data Dictionary

### Tables

**Staging Crypto**

|        Column         |       Type        |       Description    |
| :-------------------- | :---------------- | :------------------- |
| close_time    | datetime | Date when market was closed |
|     open_price    | float    | Price at which market opened |
|     high_price    |  float    | Highest price for candle interval |
|	low_price    | float    | Lowest price for candle interval |
|	close_price    |  float    | Price at which market closed |
|    volume    |  float    | Volume for candle interval denoted in base symbol |
|	quote_volume    |  float    | Volume for candle interval denoted in quote symbol |
|    id    |  int    | Unique ID for exchange and market |
|    symbol    |  varchar    | Symbol for exchange and market |
|    base_id    |  int    |  Market ID |
|    base_symbol    |  varchar    |  Market symbol |
|    base_name    |  varchar    | Market name |
|	base_fiat    |  boolean    | Is market fiat |
|	base_route    |  varchar    | Link to base table |
|	quote_id     | int    | Fiat currency ID |
|    quote_symbol     | varchar    | Fiat currency symbol |
|    quote_name     | varchar    | Fiat currency name |
|    quote_fiat     | boolean    | Is currency fiat |
|    quote_route    |  varchar    | Link to currency table |
|    route     |  varchar    |  API link |
|    markets_id     | int    | Exchange ID |
|	markets_exchange    |  varchar    |  Exchange name |
|	markets_pair    |  varchar    | `<base_symbol><quote_symbol>` |
|    markets_active    |  boolean    | Is exchange active |
|	markets_route    |  varchar    | Link to exchange table |
|	close_date     | varchar      | Date when market was closed |



**Staging News**
|        Column         |       Type        |       Description    |
| :-------------------- | :---------------- | :------------------- |
| 	author    | varchar    | Author name    |
|	content    |  varchar    | Content of article    |
|	description    |  varchar    | Description of article     |
|	publishedAt    |  datetime    | Article publish date    |
|	source_id    |  varchar    | Article source ID   |
|	source_name    |  varchar    | Article source name     |
|	title     | varchar    | Article title    |
|	url     | varchar    | Article url    |
|	urlToImage    |  varchar    | Article image url    |
|	published_date    |  varchar    | Article publish date    |
|	sentiment    |  varchar    | `POSITIVE,NEGATIVE,MIXED,NEUTRAL` |
|	positive_score    |  float    | Sentiment score    |
|	negative_score     | float    |  Sentiment score    |
|	mixed_score     | float    |  Sentiment score    |
|	neutral_score    |  float    |  Sentiment score    |


**Asset Base**
|        Column         |       Type        |       Description    |
| :-------------------- | :---------------- | :------------------- |
| id      |  int      |  Currency ID |
|	symbol      |   varchar      |  Currency symbol |
|	name       |  varchar      |  Currency name |
|	fiat      |   boolean      |  Is currency fiat |
|	route      |   varchar      |  Link to currency table       |


**Asset Quote**
|        Column         |       Type        |       Description    |
| :-------------------- | :---------------- | :------------------- |
| id      |  int      |  Market ID |
|	symbol      |   varchar      |  Market symbol |
|	name       |  varchar      |  Market name |
|	fiat      |   boolean      |  Is market fiat |
|	route      |   varchar      |  Link to market table       |


**Asset Markets**
|        Column         |       Type        |       Description    |
| :-------------------- | :---------------- | :------------------- |
| id      |  int      |  Exchange ID |
|	exchange      |   varchar      |  Exchange name |
|	pair      |  varchar      |  `<base_symbol><quote_symbol>` |
|	active      |   boolean      |  Is exchange active |
|	route      |   varchar      |  Link to exchange table       |


**Time**
|        Column         |       Type        |       Description    |
| :-------------------- | :---------------- | :------------------- |
| 	close_time      | timestamp     | Date when market was closed |
|	hour      | int     | Hour derived from close time |
|	day      | int     |  Day derived from close time |
|	week     |  int     |  Week derived from close time |
|	month     |  int     | Month derived from close time |
|	year     |  int     |  Year derived from close time |
|	weekday      | int     | Weekday derived from close time |


**Sources**
|        Column         |       Type        |       Description    |
| :-------------------- | :---------------- | :------------------- |
| id | varchar |  Article source ID |
| name | varchar |  Article source name |

**Articles**
|        Column         |       Type        |       Description    |
| :-------------------- | :---------------- | :------------------- |
| id        | int | Author ID |
|	author        |  varchar        |  Author name |
|	content        |  varchar        | Content of article |
|	description        |  varchar        | Description of article |
|	title        |  varchar       | Article title |
|	url        |  varchar        | Article url |
|	url_to_image        |  varchar        | Article image url |
|	published_at        |  datetime        | Article publish date |
|	published_date        |  varchar        | Article publish date |

**Candlestick**
|        Column         |       Type        |       Description    |
| :-------------------- | :---------------- | :------------------- |
| id          | int | Unique identifier |
|	ohlc_id          |  int          | Unique ID for exchange and market |
|	symbol          |  varchar          | Symbol for exchange and market |
|	base_id          |   int          | Market ID |
|	quote_id          |  int          | Fiat Currency ID |
|	route          |  varchar          | API link |
|	markets_id          |  int          | Exchange ID |
|	close_date          |  varchar          | Date when market was closed |
| close_time    | datetime | Date when market was closed |
|     open_price    | float    | Price at which market opened |
|     high_price    |  float    | Highest price for candle interval |
|	low_price    | float    | Lowest price for candle interval |
|	close_price    |  float    | Price at which market closed |
|    volume    |  float    | Volume for candle interval denoted in base symbol |
|	quote_volume    |  float    | Volume for candle interval denoted in quote symbol |
|	article_id          |  int          | Unique article ID |
|	source_id          |  varchar          | Unique source ID |
|	sentiment          |  varchar          | `POSITIVE,NEGATIVE,MIXED,NEUTRAL` |
|	positive_score    |  float    | Sentiment score    |
|	negative_score     | float    |  Sentiment score    |
|	mixed_score     | float    |  Sentiment score    |
|	neutral_score    |  float    |  Sentiment score    |
