create table country
(
    id              bigserial primary key,
    name            varchar(255),
    government_form varchar(255),
    population      int
);

create table city
(
    id         bigserial primary key,
    country_id bigint references country(id),
    name       varchar(255),
    population int
);