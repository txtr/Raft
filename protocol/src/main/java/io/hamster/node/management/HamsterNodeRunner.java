package io.hamster.node.management;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node Runner
 */
public class HamsterNodeRunner {

    /**
     * Runs a standalone server from the given command line arguments.
     *
     * @param args the program arguments
     * @throws Exception if the supplied arguments are invalid
     */
    public static void main(String[] args) throws Exception {
        // Parse the command line arguments.
        final Namespace namespace = parseArgs(args);
        Logger logger = createLogger();
        logger.info("Node ID: {}", namespace.getString("node"));
    }

    /**
     * Configures and creates a new logger for the given namespace.
     *
     * @return a new agent logger
     */
    static Logger createLogger() {
        if (System.getProperty("hamster.log.directory") == null) {
            System.setProperty("hamster.log.directory", "log");
        }
        if (System.getProperty("hamster.log.level") == null) {
            System.setProperty("hamster.log.level", "INFO");
        }
        if (System.getProperty("hamster.log.file.level") == null) {
            System.setProperty("hamster.log.file.level", "INFO");
        }
        if (System.getProperty("hamster.log.console.level") == null) {
            System.setProperty("hamster.log.console.level", "INFO");
        }
        return LoggerFactory.getLogger(HamsterNodeRunner.class);
    }

    /**
     * Parses the command line arguments, returning an argparse4j namespace.
     *
     * @param args the arguments to parse
     * @return the namespace
     */
    static Namespace parseArgs(String[] args) {
        ArgumentParser parser = createParser();
        Namespace namespace = null;
        try {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        return namespace;
    }

    /**
     * Creates an agent argument parser.
     */
    private static ArgumentParser createParser() {
        final ArgumentParser parser = ArgumentParsers.newArgumentParser("Hamster Server")
                .defaultHelp(true)
                .description("Runs the hamster server with the given arguments.");
        parser.addArgument("node")
                .metavar("ID")
                .type(String.class)
                .required(true)
                .help("The local node ID.");

        return parser;
    }
}
