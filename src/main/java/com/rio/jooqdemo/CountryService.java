package com.rio.jooqdemo;

import org.jooq.DSLContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import static com.rio.jooqdemo.domain.Tables.COUNTRY;

@Service
public class CountryService {

  private final DSLContext dsl;

  public CountryService(DSLContext dsl) {
    this.dsl = dsl;
  }

  @Transactional
  public void saveNew() {
    dsl.insertInto(COUNTRY, COUNTRY.NAME, COUNTRY.GOVERNMENT_FORM, COUNTRY.POPULATION)
            .values("Ukraine", "UNITARY", 45_000_000)
            .execute();

    throw new RuntimeException("Rollback!");
  }
}
