package core.utils;

import org.apache.log4j.*;
import org.graylog2.log.GelfAppender;
import scala.App;

import java.util.Enumeration;

public class LoggerHandler {

    private static LoggerHandler instance;
    private static boolean loggingIsInitialized = false;
    private static Logger rootLogger;
    private static Level defaultLevel;
    private static PatternLayout layout;

    public static LoggerHandler getInstance() {
        if (instance==null)
            instance = new LoggerHandler();
        return instance;
    }

    private LoggerHandler() {
        defaultLevel = Level.DEBUG;
        layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");
        if(!loggingIsInitialized)
            initLogging();
    }

    private static void initLogging() {
        BasicConfigurator.configure();
        rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.DEBUG);
        rootLogger.removeAllAppenders();
        rootLogger.addAppender(generateConsoleAppender());
        rootLogger.addAppender(generateGelfAppender());
        loggingIsInitialized = true;
    }


    private static Appender generateGelfAppender(){
        GelfAppender appender = new GelfAppender();
        appender.setName("GrayLogAppender");
        appender.setGraylogHost("127.0.0.1");
        appender.setOriginHost("127.0.0.1");
        // appender.setGraylogPort(12201);
        appender.setFacility("gelf-java");
        appender.setOriginHost("localhost");
        appender.setLayout(layout);
        appender.setExtractStacktrace(true);
        appender.setAddExtendedInformation(true);
        appender.setAdditionalFields("{'environment': 'DEV', 'application':'MyAPP'}");
        appender.activateOptions();
        return appender;
    }

    private static Appender generateConsoleAppender(){
        ConsoleAppender appender = new ConsoleAppender();
        appender.setName("ConsoleAppender");
        appender.setTarget("System.out");
        appender.setLayout(layout);
        //appender.setLayout(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN));
        appender.activateOptions();
        return appender;
    }

    public static Logger getRootLogger() {
        return rootLogger;
    }

    /*
        <appender name="graylog" class="org.graylog2.log.GelfAppender">
        <param name="graylogHost" value="localhost"/>
        <param name="originHost" value="localhost"/>
        <param name="graylogPort" value="12201"/>
        <param name="extractStacktrace" value="true"/>
        <param name="addExtendedInformation" value="true"/>
        <param name="facility" value="log4j"/>
        <param name="Threshold" value="DEBUG"/>
        <param name="additionalFields" value="{'environment': 'DEV', 'application': 'GraylogDemoApplication'}"/>
    </appender>
     */

}
