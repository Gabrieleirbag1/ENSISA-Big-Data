package TD1.moi;

import java.io.File;
import java.net.URI;
import java.util.List;

import static TD1.moi.utils.Utils.*;

import TD1.moi.utils.CloseableIterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("TD1");

		String master = System.getProperty("spark.master");
		if (master == null || master.trim().length() == 0) {
			System.out.println("No master found ; running locally");
			conf.setMaster("local[*]")
				.set("spark.driver.host", "127.0.0.1")
				;
		} else {
			System.out.println("Master found to be " + master);
		}

		// Tries to determine necessary jar
		String source = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (source.endsWith(".jar")) {
			conf.setJars(new String [] {source});
		}

		File inFile = new File("data-2018.csv");
		downloadIfAbsent(new URI("https://svn.ensisa.uha.fr/bd/co2/owid-co2-data-continents-2018.csv").toURL(), inFile);

		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			
			JavaRDD<String> raw = sc.textFile(inFile.getAbsolutePath(), 14*8);
			List<String> res = raw.take(10);
			res.stream().forEach(System.out::println);
		}

		long s = (System.currentTimeMillis() - start) / 1000;
		String dur = String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
		System.out.println("Analysis completed in " + dur);
	}
}
