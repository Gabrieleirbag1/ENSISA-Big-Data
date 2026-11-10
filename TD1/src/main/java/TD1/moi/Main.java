package TD1.moi;

import static TD1.moi.utils.Utils.downloadIfAbsent;
import static TD1.moi.utils.Utils.fromCSV;

import java.io.File;
import java.net.URI;

import scala.Tuple2;
import scala.Tuple4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	private static final int IDX_COUNTRY = 0;
	private static final int IDX_YEAR = 1;
	private static final int IDX_CONTINENT = 3;
	private static final int IDX_GDP = 5;
	private static final int IDX_TOTAL_GHG = 76;

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("ghg");

		String master = System.getProperty("spark.master");
		if (master == null || master.trim().length() == 0) {
			System.out.println("No master found ; running locally");
			conf.setMaster("local[*]")
				.set("spark.driver.host", "127.0.0.1");
		} else {
			System.out.println("Master found to be " + master);
		}

		String source = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (source.endsWith(".jar")) {
			conf.setJars(new String[] { source });
		}

		File file2018 = new File("ghg2018.csv");
		File file2022 = new File("ghg2022.csv");
		downloadIfAbsent(new URI("https://svn.ensisa.uha.fr/bd/co2/owid-co2-data-continents-2018.csv").toURL(), file2018);
		downloadIfAbsent(new URI("https://svn.ensisa.uha.fr/bd/co2/owid-co2-data-continents-2022.csv").toURL(), file2022);

		try (JavaSparkContext sc = new JavaSparkContext(conf)) {

			// Découplage = PIB / GES, calculé en entiers.
			// Résultat : (continent, pays, année, découplage), uniquement 2018 et 2022.
			JavaRDD<Tuple4<String, String, String, Long>> decoupling = sc
				.textFile(file2018.getAbsolutePath() + "," + file2022.getAbsolutePath(), 14 * 5)
				.filter(l -> !l.startsWith("country,year,"))
				.map(l -> fromCSV(l, ','))
				.filter(c -> c.length > IDX_TOTAL_GHG)
				.filter(c -> c[IDX_YEAR].equals("2018") || c[IDX_YEAR].equals("2022"))
				.filter(c -> !c[IDX_CONTINENT].isBlank())
				.filter(c -> !c[IDX_COUNTRY].isBlank())
				.filter(c -> !c[IDX_TOTAL_GHG].isBlank())
				.filter(c -> !c[IDX_GDP].isBlank())
				.map(c -> {
					// GES : format XX.XXX -> entier (milliers de tonnes)
					int ghg = -1;
					String ghgAsStr = c[IDX_TOTAL_GHG];
					int coma = ghgAsStr.indexOf('.');
					if (coma >= 0 && ghgAsStr.length() - coma - 1 == 3) {
						try {
							ghg = Integer.parseInt(ghgAsStr.substring(0, coma) + ghgAsStr.substring(coma + 1));
						} catch (NumberFormatException x) {}
					}

					// PIB : partie entière en millions
					int gdp = -1;
					String gdpAsStr = c[IDX_GDP];
					coma = gdpAsStr.indexOf('.');
					if (coma < 0) coma = gdpAsStr.length() - 6;
					else coma -= 6;
					if (coma > 0) {
						try {
							gdp = Integer.parseInt(gdpAsStr.substring(0, coma));
						} catch (NumberFormatException x) {}
					}

					long ratio = (ghg > 0 && gdp > 0) ? ((long) gdp * 1_000L) / ghg : -1L;
					return new Tuple4<String, String, String, Long>(c[IDX_CONTINENT], c[IDX_COUNTRY], c[IDX_YEAR], ratio);
				})
				.filter(c -> c._4() > 0);

			// pays -> (continent, découplage 2018)
			JavaPairRDD<String, Tuple2<String, Long>> d2018 = decoupling
				.filter(c -> c._3().equals("2018"))
				.mapToPair(c -> new Tuple2<String, Tuple2<String, Long>>(c._2(), new Tuple2<String, Long>(c._1(), c._4())));

			// pays -> découplage 2022
			JavaPairRDD<String, Long> d2022 = decoupling
				.filter(c -> c._3().equals("2022"))
				.mapToPair(c -> new Tuple2<String, Long>(c._2(), c._4()));

			System.out.println();
			System.out.printf("%-15s | %-20s | %-15s | %-15s | %s%n",
				"Continent", "Pays", "Découplage 2018", "Découplage 2022", "Amélioration (%)");
			System.out.println("-".repeat(95));

			d2018
				.join(d2022)
				// continent -> (pays, d2018, d2022, amélioration en % entier)
				.mapToPair(t -> {
					long a = t._2()._1()._2();
					long b = t._2()._2();
					return new Tuple2<String, Tuple4<String, Long, Long, Long>>(
						t._2()._1()._1(),
						new Tuple4<String, Long, Long, Long>(t._1(), a, b, (b - a) * 100L / a));
				})
				.reduceByKey((x, y) -> x._4() >= y._4() ? x : y)
				.sortByKey()
				.collect() // seule action
				.forEach(t -> System.out.printf("%-15s | %-20s | %-15d | %-15d | %+d%n",
					t._1(), t._2()._1(), t._2()._2(), t._2()._3(), t._2()._4()));
		}

		long s = (System.currentTimeMillis() - start) / 1000;
		String dur = String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
		System.out.println("Analysis completed in " + dur);
	}
}