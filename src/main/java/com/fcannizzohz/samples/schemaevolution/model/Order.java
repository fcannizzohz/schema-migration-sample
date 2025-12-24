package com.fcannizzohz.samples.schemaevolution.model;

import java.math.BigDecimal;

public record Order(long id, long customerId, BigDecimal amount, String status) {}

