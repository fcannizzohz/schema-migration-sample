package com.fcannizzohz.samples.schemaevolution.model;

import java.math.BigDecimal;

public record OrderV3(
        long id,
        long accountId,  // semantic change
        BigDecimal amount,
        String status,
        String currency) {}

