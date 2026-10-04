package com.aliyun.odps.kafka.connect.utils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.SimpleJsonValue;

public class ConverterHelper {

  public static void setRecordByType(ArrayRecord rec, int idx, String value) throws ParseException {
    if (value == null) {
      rec.set(idx, null);
      return;
    }
    switch (rec.getColumns()[idx].getTypeInfo().getOdpsType()) {
      case STRING:
        rec.setString(idx, value);
        break;
      case BIGINT:
        rec.setBigint(idx, Long.valueOf(value));
        break;
      case DOUBLE:
        switch (value) {
          case "nan":
            rec.setDouble(idx, Double.NaN);
            break;
          case "inf":
            rec.setDouble(idx, Double.POSITIVE_INFINITY);
            break;
          case "-inf":
            rec.setDouble(idx, Double.NEGATIVE_INFINITY);
            break;
          default:
            rec.setDouble(idx, Double.valueOf(value));
            break;
        }
        break;
      case DATE:
        // TODO add Time zone config, now use system default
        rec.setDateAsLocalDate(idx, LocalDate.parse(value));
        break;
      case TIMESTAMP:
        // TODO add Time zone config, now use system default
        DateTimeFormatter
            TsFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS")
                .withZone(ZoneId.systemDefault());
        Instant instant = ZonedDateTime.parse(value, TsFormatter).toInstant();
        rec.setTimestampAsInstant(idx, instant);
        break;
      case BOOLEAN:
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
          throw new IllegalArgumentException("Invalid BOOLEAN value; expected true or false");
        }
        rec.setBoolean(idx, Boolean.valueOf(value));
        break;
      case DATETIME:
        // TODO add Time zone config, now use system default
        DateTimeFormatter
            DtFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(value, DtFormatter);
        rec.setDatetimeAsZonedDateTime(idx, zonedDateTime);
        break;
      case DECIMAL:
        rec.setDecimal(idx, new BigDecimal(value));
        break;
      case JSON:
        rec.setJsonValue(idx, new SimpleJsonValue(value));
        break;
      default:
        throw new RuntimeException("Unsupported type " +
                                   rec.getColumns()[idx].getTypeInfo().getOdpsType());
    }
  }
}
