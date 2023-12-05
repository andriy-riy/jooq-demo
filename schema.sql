create table countries
(
    id              bigserial primary key,
    name            varchar(255),
    government_form varchar(255),
    population      int
);

create table cities
(
    id         bigserial primary key,
    country_id bigint,
    name       varchar(255),
    population int
);