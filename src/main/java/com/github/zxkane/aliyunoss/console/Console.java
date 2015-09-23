package com.github.zxkane.aliyunoss.console;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;

import com.github.zxkane.aliyunoss.AliyunOSSFS;

import jline.console.ConsoleReader;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;
import net.fusejna.FuseException;

public class Console {

	public void run(final InputStream inStream, final OutputStream outStream) throws IOException {
		ConsoleReader reader = new ConsoleReader("AliyunOSSFS", inStream, outStream, null);

		reader.setPrompt("aliyun-oss-fs> ");

		reader.addCompleter(new FileNameCompleter());
		reader.addCompleter(new StringsCompleter(Arrays.asList(new String[] { "mount", "unmount", "list", "exit", "quit", "cls", })));

		// TODO: the completers do not seem to work, is there more to do to make
		// them work?

		String line;
		PrintWriter out = new PrintWriter(reader.getOutput());

		while ((line = reader.readLine()) != null) {
			if (line.startsWith("mount")) {
				String[] cmd = line.split("\\s+");
				if (cmd.length < 3) {
					out.println("Invalid command");
					help(out);
				} else {
					try {
						AliyunOSSFS.mount(cmd[1], new File(cmd[2]));
					} catch (IllegalArgumentException | IllegalStateException | IOException | FuseException e) {
						e.printStackTrace(out);
					}
				}
			} else if (line.startsWith("unmount") || line.startsWith("umount")) {
				String[] cmd = line.split("\\s+");
				if (cmd.length < 2) {
					out.println("Invalid command");
					help(out);
				} else {
					// out.println("Umounting " + cmd[1]);
					try {
						AliyunOSSFS.unmount(cmd[1]);
					} catch (IOException e) {
						e.printStackTrace(out);
					}
				}
			} else if (line.startsWith("list")) {
				AliyunOSSFS.list();
			} else {
				help(out);
			}

			if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
				break;
			}
			if (line.equalsIgnoreCase("cls")) {
				reader.clearScreen();
			}
		}
	}

	private void help(PrintWriter out) {
		out.println("mount <git-dir> <mountpoint>");
		out.println("umount <git-dir>|<mountpoint>");
		out.println("list ... list current mounts");
		out.println("quit ... quit the applicatoin");
		out.println("exit ... quit the application");
		out.println("cls  ... clear the screen");
	}
}
