package com.fcannizzohz.samples.schemaevolution.model;

import java.math.BigDecimal;

public record OrderV2(long id, long customerId, BigDecimal amount, String status, String currency) {}

