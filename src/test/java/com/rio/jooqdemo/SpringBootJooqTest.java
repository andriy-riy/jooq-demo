package com.rio.jooqdemo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class SpringBootJooqTest {

  @Autowired
  private CountryService countryService;

  @Test
  public void testTransactional() {
    countryService.saveNew();
  }
}
