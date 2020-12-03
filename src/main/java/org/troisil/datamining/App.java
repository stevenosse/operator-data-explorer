package org.troisil.datamining;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Hello world!
 *
 */
@Slf4j
public class App 
{
    public static void main( String[] args )
    {
        Config config = ConfigFactory.load("application.conf");
        log.info("See! It's logging.");
    }
}
