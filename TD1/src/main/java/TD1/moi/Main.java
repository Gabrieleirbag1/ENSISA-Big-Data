package TD1.moi;

import static TD1.moi.utils.Utils.downloadIfAbsent;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import scala.Tuple2;
import scala.Tuple4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	private static final int IDX_CONTINENT = 3;
	private static final int IDX_COUNTRY = 0;
	private static final int IDX_TOTAL_GHG = 76;
	private static final int IDX_GDP = 5;

	static String[] fromCSV(String line, char sep) {
		List<String> fields = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		boolean inQuotes = false;
		for (int i = 0; i < line.length(); i++) {
			char ch = line.charAt(i);
			if (inQuotes) {
				if (ch == '"') {
					if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
						sb.append('"');
						i++;
					} else {
						inQuotes = false;
					}
				} else {
					sb.append(ch);
				}
			} else if (ch == '"') {
				inQuotes = true;
			} else if (ch == sep) {
				fields.add(sb.toString());
				sb.setLength(0);
			} else {
				sb.append(ch);
			}
		}
		fields.add(sb.toString());
		return fields.toArray(new String[0]);
	}

	/** Pour chaque continent : (pays, PIB par unité de GES) maximal pour l'année donnée. */
	static Map<String, Tuple2<String, Long>> analyse(JavaSparkContext sc, int year) throws Exception {
		File inFile = new File("ghg" + year + ".csv");
		downloadIfAbsent(new URI("https://svn.ensisa.uha.fr/bd/co2/owid-co2-data-continents-" + year + ".csv").toURL(), inFile);

		JavaRDD<String> raw = sc.textFile(inFile.getAbsolutePath(), 14 * 5);

		JavaRDD<Tuple4<String, String, String, String>> rawCurated = raw
			.filter(l -> !l.startsWith("country,year,"))
			.map(l -> fromCSV(l, ','))
			.filter(c -> c.length > IDX_TOTAL_GHG)
			.map(c -> new Tuple4<String, String, String, String>(c[IDX_CONTINENT], c[IDX_COUNTRY], c[IDX_TOTAL_GHG], c[IDX_GDP]))
			.filter(c -> !c._1().isBlank())
			.filter(c -> !c._2().isBlank())
			.filter(c -> !c._3().isBlank())
			.filter(c -> !c._4().isBlank());

		JavaRDD<Tuple4<String, String, Integer, Integer>> countries = rawCurated
			.map(c -> {
				String ghgAsStr = c._3();
				int coma = ghgAsStr.indexOf('.');
				int ghg = -1;
				if (coma >= 0) { // format should be XX.XXX
					int decimals = ghgAsStr.length() - coma - 1;
					if (decimals == 3) {
						ghgAsStr = ghgAsStr.substring(0, coma) + ghgAsStr.substring(coma + 1);
						try {
							ghg = Integer.parseInt(ghgAsStr);
						} catch (NumberFormatException x) {}
					}
				}
				return new Tuple4<String, String, Integer, String>(c._1(), c._2(), ghg, c._4());
			})
			.map(c -> {
				String gdpAsStr = c._4();
				int gdp = -1;
				int coma = gdpAsStr.indexOf('.');
				if (coma < 0) coma = gdpAsStr.length() - 6;
				else coma -= 6;
				if (coma > 0) {
					gdpAsStr = gdpAsStr.substring(0, coma);
					try {
						gdp = Integer.parseInt(gdpAsStr);
					} catch (NumberFormatException x) {}
				}
				return new Tuple4<String, String, Integer, Integer>(c._1(), c._2(), c._3(), gdp);
			})
			.filter(c -> c._3() > 0 && c._4() >= 0); // évite division par zéro / valeurs invalides

		JavaPairRDD<String, Tuple2<String, Long>> result = countries
			.mapToPair(c -> new Tuple2<String, Tuple2<String, Long>>(
				c._1(),
				new Tuple2<String, Long>(c._2(), ((long) c._4() * 1_000L) / c._3())))
			.reduceByKey((c1, c2) -> c1._2() > c2._2() ? c1 : c2);

		return new TreeMap<>(result.collectAsMap()); // trié par continent
	}

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

		// Tries to determine necessary jar
		String source = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (source.endsWith(".jar")) {
			conf.setJars(new String[] { source });
		}

		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			Map<String, Tuple2<String, Long>> r2018 = analyse(sc, 2018);
			Map<String, Tuple2<String, Long>> r2022 = analyse(sc, 2022);

			System.out.println("\n=== 2018 ===");
			r2018.forEach((k, v) -> System.out.println(k + " -> " + v));
			System.out.println("\n=== 2022 ===");
			r2022.forEach((k, v) -> System.out.println(k + " -> " + v));

			System.out.println("\n=== Comparatif ===");
			System.out.printf("%-15s | %-22s | %-22s | %s%n", "Continent", "2018", "2022", "Évolution");
			TreeMap<String, Boolean> continents = new TreeMap<>();
			r2018.keySet().forEach(k -> continents.put(k, true));
			r2022.keySet().forEach(k -> continents.put(k, true));
			for (String cont : continents.keySet()) {
				Tuple2<String, Long> a = r2018.get(cont);
				Tuple2<String, Long> b = r2022.get(cont);
				String sa = a == null ? "n/a" : a._1() + " (" + a._2() + ")";
				String sb = b == null ? "n/a" : b._1() + " (" + b._2() + ")";
				String evo = (a == null || b == null) ? "n/a"
					: String.format("%+.1f %%", 100.0 * (b._2() - a._2()) / a._2());
				String same = (a != null && b != null && a._1().equals(b._1())) ? " [même pays]" : "";
				System.out.printf("%-15s | %-22s | %-22s | %s%s%n", cont, sa, sb, evo, same);
			}
		}

		long s = (System.currentTimeMillis() - start) / 1000;
		String dur = String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
		System.out.println("Analysis completed in " + dur);
	}
}