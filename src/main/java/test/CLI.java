package test;

import java.io.InputStream;
import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;

public class CLI {

	/**
	 * @param args
	 *            输入参数
	 */
	public static void main(String[] args) {
		simpleTest(args);
	}

	public static void simpleTest(String[] args) {
		
		ResourceBundle resourceBundle = ResourceBundle.getBundle("message",
				Locale.getDefault());
		Options opts = new Options();
		opts.addOption("h", false, resourceBundle.getString("HELP_DESCRIPTION"));
		opts.addOption("i", true, resourceBundle.getString("HELP_IPADDRESS"));
		opts.addOption("p", true, resourceBundle.getString("HELP_PORT"));
		opts.addOption("t", true, resourceBundle.getString("HELP_PROTOCOL"));
		BasicParser parser = new BasicParser();
		CommandLine cl;
		try {
			cl = parser.parse(opts, args);
			if (cl.getOptions().length > 0) {
				if (cl.hasOption('h')) {
					HelpFormatter hf = new HelpFormatter();
					hf.printHelp("Options", opts);
				} else {
					String ip = cl.getOptionValue("i");
					String port = cl.getOptionValue("p");
					String protocol = cl.getOptionValue("t");
				}
			} else {
				System.err.println(resourceBundle.getString("ERROR_NOARGS"));
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
