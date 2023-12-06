package com.rio.jooqdemo;

import com.rio.jooqdemo.domain.tables.Country;
import com.rio.jooqdemo.domain.tables.records.CountryRecord;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Records;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static com.rio.jooqdemo.domain.Tables.CITY;
import static com.rio.jooqdemo.domain.Tables.COUNTRY;
import static org.jooq.impl.DSL.choose;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.excluded;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.val;

class JooqDemoTests {

  private static final String URL = "jdbc:postgresql://localhost:5432/jooq-demo";
  private static final String USERNAME = "admin";
  private static final String PASSWORD = "admin";

  private final Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
  private final DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES, new Settings()
          .withRenderFormatted(true)
          .withMapConstructorParameterNames(true)
  );

  JooqDemoTests() throws SQLException {
  }

  @Test
  void insert() {
    /*
      insert into country(name, government_form, population)
      values ('Ukraine', 'UNITARY', 45000000);
     */
    dsl.insertInto(COUNTRY, COUNTRY.NAME, COUNTRY.GOVERNMENT_FORM, COUNTRY.POPULATION)
            .values("Ukraine", "UNITARY", 45_000_000)
            .execute();


//    CountryRecord inserted = dsl.insertInto(COUNTRY, COUNTRY.NAME, COUNTRY.GOVERNMENT_FORM, COUNTRY.POPULATION)
//            .values("Ukraine", "UNITARY", 45_000_000)
//            .returning()
//            .fetchOneInto(Country.COUNTRY);
//
//    System.out.println(inserted);


//    var country = new CountryDTO("Ukraine", "UNITARY", 45_000_000);
//
//    dsl.insertInto(COUNTRY)
//            .set(dsl.newRecord(COUNTRY, country))
//            .execute();

    /*
      insert into country(name, government_form, population)
      values ('Ukraine', 'UNITARY', 45000000)
             ('Poland', 'UNITARY', 35000000)
             ('France', 'UNITARY', 68000000);
     */
//    dsl.insertInto(COUNTRY, COUNTRY.NAME, COUNTRY.GOVERNMENT_FORM, COUNTRY.POPULATION)
//            .values("Ukraine", "UNITARY", 45_000_000)
//            .values("Poland", "UNITARY", 35_000_000)
//            .values("France", "UNITARY", 68_000_000)
//            .execute();
  }

  @Test
  void batchInsert() {
    List<CountryDTO> countriesDto = List.of(
            new CountryDTO("Ukraine", "UNITARY", 45_000_000),
            new CountryDTO("Poland", "UNITARY", 35_000_000),
            new CountryDTO("France", "UNITARY", 68_000_000)
    );

    List<CountryRecord> records = countriesDto.stream()
            .map(countryDTO -> dsl.newRecord(COUNTRY, countryDTO))
            .toList();

    dsl.batchInsert(records).execute();
  }

  @Test
  void select() {
    /*
    select * from country
    where population > 30000000 and government_form != 'FEDERATION';
     */
    List<CountryRecord> countries = dsl.selectFrom(COUNTRY)
            .where(COUNTRY.POPULATION.greaterThan(30_000_000))
            .and(COUNTRY.GOVERNMENT_FORM.notEqual("FEDERATION"))
            .fetchInto(CountryRecord.class);

    System.out.println(countries);


//    Result<Record2<String, String>> records = dsl.select(COUNTRY.NAME, COUNTRY.GOVERNMENT_FORM)
//            .from(COUNTRY)
//            .where(COUNTRY.POPULATION.greaterThan(30_000_000))
//            .and(COUNTRY.GOVERNMENT_FORM.notEqual("FEDERATION"))
//            .fetch();
//
//    System.out.println(records);


//    Result<Record> countryWithCities = dsl.select()
//            .from(COUNTRY.join(CITY)
//                    .on(CITY.COUNTRY_ID.eq(COUNTRY.ID)))
//            .where(COUNTRY.POPULATION.greaterThan(30_000_000))
//            .and(COUNTRY.GOVERNMENT_FORM.notEqual("FEDERATION"))
//            .and(CITY.NAME.like("%Lv%"))
//            .fetch();
//
//    System.out.println(countryWithCities);
  }

  @Test
  void aggregation() {
    Result<Record2<Object, Integer>> result = dsl.select(field("range"), count())
            .from(dsl.select(choose()
                            .when(COUNTRY.POPULATION.between(0, 1_000_000), val("0 - 1 000 000"))
                            .when(COUNTRY.POPULATION.between(1_000_000, 10_000_000), val("1 000 000 - 10 000 000"))
                            .when(COUNTRY.POPULATION.between(10_000_000, 50_000_000), val("10 000 000 - 50 000 000"))
                            .when(COUNTRY.POPULATION.between(50_000_000, 200_000_000), val("50 000 000 - 200 000 000"))
                            .else_(val("> 200 000 000"))
                            .as("range"))
                    .from(COUNTRY)
            )
            .groupBy(field("range"))
            .fetch();

    System.out.println(result);
  }

  @Test
  void update() {
    dsl.update(COUNTRY)
            .set(COUNTRY.GOVERNMENT_FORM, "FEDERATION")
            .set(COUNTRY.POPULATION, 40_000_000)
            .where(COUNTRY.ID.eq(1L))
            .execute();


//    var country = new CountryDTO("Ukraine", "FEDERATION", 40_000_000);
//
//    dsl.update(COUNTRY)
//            .set(dsl.newRecord(COUNTRY, country))
//            .where(COUNTRY.ID.eq(1L))
//            .execute();


//    dsl.update(COUNTRY)
//            .set(COUNTRY.POPULATION, CITY.POPULATION)
//            .from(CITY)
//            .where(CITY.COUNTRY_ID.eq(COUNTRY.ID))
//            .execute();
  }

  @Test
  void upsert() {
    var country = new CountryRecord(1L, "Ukraine", "UNITARY", 45_000_000);

    dsl.insertInto(COUNTRY)
            .set(country)
            .onDuplicateKeyUpdate()
            .set(COUNTRY.GOVERNMENT_FORM, country.getGovernmentForm())
            .set(COUNTRY.POPULATION, country.getPopulation())
            .execute();

//    dsl.insertInto(COUNTRY)
//            .set(country)
//            .onConflict()
//            .doUpdate()
//            .set(COUNTRY.GOVERNMENT_FORM, country.getGovernmentForm())
//            .set(COUNTRY.POPULATION, country.getPopulation())
//            .execute();
  }

  @Test
  void delete() {
    dsl.deleteFrom(COUNTRY)
            .where(COUNTRY.POPULATION.greaterThan(100_000_000)
                    .or(COUNTRY.GOVERNMENT_FORM.eq("FEDERATION")))
            .execute();

//    CountryRecord country = dsl.deleteFrom(COUNTRY)
//            .where(COUNTRY.ID.eq(44L))
//            .returning()
//            .fetchOne();
//
//    System.out.println(country);

//    dsl.deleteFrom(COUNTRY)
//            .orderBy(COUNTRY.POPULATION.desc())
//            .limit(2)
//            .execute();
  }

  record CountryDTO(String name, String governmentForm, Integer population) {
  }
}
