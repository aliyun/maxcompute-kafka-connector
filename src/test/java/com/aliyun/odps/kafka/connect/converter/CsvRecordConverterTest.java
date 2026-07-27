package com.aliyun.odps.kafka.connect.converter;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class CsvRecordConverterTest {

    @Test
    public void parsesCommaDelimitedRecordsByDefault() throws IOException {
        Assert.assertArrayEquals(
            new String[] {"first", "second", "third"},
            CsvRecordConverter.load("first,second,third", ','));
    }

    @Test
    public void parsesTabDelimitedRecords() throws IOException {
        Assert.assertArrayEquals(
            new String[] {"first", "second", "third"},
            CsvRecordConverter.load("first\tsecond\tthird", '\t'));
    }

    @Test
    public void acceptsTabCharacterAndEscapedTabConfiguration() {
        Assert.assertEquals('\t', CsvRecordConverter.parseDelimiter("\t"));
        Assert.assertEquals('\t', CsvRecordConverter.parseDelimiter("\\t"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsMultiCharacterDelimiter() {
        CsvRecordConverter.parseDelimiter("||");
    }
}
