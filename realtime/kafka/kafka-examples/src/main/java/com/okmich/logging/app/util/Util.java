package com.okmich.logging.app.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Util {

	public static String parseLog(String logEntry) {

		String REGEX = "(.+?)\\s(.+?)\\s(.+?)\\s\\[(.+?)\\s(.+?)\\]\\s\"(.+?)\\s(.+?)\\s(.+?)\"\\s(.+?)\\s(.+?)\\n";
		Pattern pattern = Pattern.compile(REGEX);
		Matcher matcher = pattern.matcher(logEntry);
		if (matcher.matches()) {
			return String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s",
					matcher.group(1), matcher.group(4), matcher.group(5),
					matcher.group(6), matcher.group(7), matcher.group(9),
					matcher.group(10));
		} else {
			return "";
		}
	}

	public static String getResourceValue(String arg) {
		// 2017-09-16 14/Aug/1995:01:16:58 -0400 GET /images/USA-logosmall.gif
		// 200 234
		if (arg.isEmpty())
			return "";

		String[] parts = arg.split("\t");
		if (parts.length < 5)
			return "";
		else
			return parts[4];

	}

	// public static void main(String[] arg) {
	//
	// System.out
	// .println(parseLog("prusec.prusec.com - - [03/Aug/1995:13:59:45 -0400] \"GET /software/winvn/wvsmall.gif HTTP/1.0\" 200 13372\n"));
	// System.out
	// .println(parseLog("2017-09-16 21:34:27 INFO  Main:59 - supreme.pcug.org.au - - [11/Aug/1995:23:03:19 -0400] \"GET /shuttle/countdown/ HTTP/1.0\" 304 0\n"));
	// }
}
