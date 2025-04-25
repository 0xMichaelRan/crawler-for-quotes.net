-- quotesnet.movies definition

-- Drop table

-- DROP TABLE quotesnet.movies;

CREATE TABLE quotesnet.movies (
	id serial4 NOT NULL,
	title text NOT NULL,
	"year" int4 NULL,
	movie_id int4 NULL,
	url text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT movies_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_movie_movie_id ON quotes.movies USING btree (movie_id);
CREATE INDEX idx_movie_title ON quotes.movies USING btree (title);
CREATE INDEX idx_movie_year ON quotes.movies USING btree (year);


-- quotesnet."quotes" definition

-- Drop table

-- DROP TABLE quotesnet."quotes";

CREATE TABLE quotesnet."quotes" (
	id serial4 NOT NULL,
	movie_id int4 NULL,
	quote_text text NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT quotes_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_quote_movie_id ON quotes.quotes USING btree (movie_id);