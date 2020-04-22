package io.datasearch.denguestore;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.data.DataAccessFactory;

import java.util.HashMap;
import java.util.Map;

public class CommandLineDataStore {

    private CommandLineDataStore() {}

    public static Options createOptions(DataAccessFactory.Param[] parameters) {
        Options options = new Options();
        for (DataAccessFactory.Param p: parameters) {
            if (!p.isDeprecated()) {
                Option opt = Option.builder(null)
                        .longOpt(p.getName())
                        .argName(p.getName())
                        .hasArg()
                        .desc(p.getDescription().toString())
                        .required(p.isRequired())
                        .build();
                options.addOption(opt);
            }
        }
        return options;
    }

    public static CommandLine parseArgs(Class<?> caller, Options options, String[] args) throws ParseException {
        try {
            return new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(caller.getName(), options);
            throw e;
        }
    }

    public static Map<String, String> getDataStoreParams(CommandLine command, Options options) {
        Map<String, String> params = new HashMap<>();
        // noinspection unchecked
        for (Option opt: options.getOptions()) {
            String value = command.getOptionValue(opt.getLongOpt());
            if (value != null) {
                params.put(opt.getArgName(), value);
            }
        }

        return params;
    }
}
