package org.apache.hadoop.hdfs.util;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.helpers.PatternParser;
import org.apache.log4j.spi.LoggingEvent;

/**
 * This class extends {@link PatternLayout} class. This adds an extra character 'i'
 * to the pattern-layout in order to enable log4j to display thread-ids in the logs.
 * 
 * @author Hooman <hooman@sics.se>
 */
public class KthfsPatternLayout extends PatternLayout {

  @Override
  protected PatternParser createPatternParser(String pattern) {
    return new KthfsPatternParser(pattern);
  }

  public class KthfsPatternParser extends PatternParser {

    private static final char THREAD_ID = 'i';

    public KthfsPatternParser(String pattern) {
      super(pattern);
    }

    @Override
    protected void finalizeConverter(char c) {
      switch (c) {
        case THREAD_ID:
          currentLiteral.setLength(0);
          addConverter(new PatternConverter() {

            @Override
            protected String convert(LoggingEvent le) {
              return String.valueOf(Thread.currentThread().getId());
            }
          });
          break;
        default:
          super.finalizeConverter(c);
      }
    }
  }
}
