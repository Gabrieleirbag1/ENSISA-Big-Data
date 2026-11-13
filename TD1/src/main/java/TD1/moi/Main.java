package TD1.moi;

import static TD1.moi.utils.Utils.downloadIfAbsent;
import static TD1.moi.utils.Utils.fromCSV;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class Main {

	private static final List<String> CODES = List.of("AF", "AS", "EU", "NA", "OC", "SA");

	// continents.txt -> table (nom de pays ou code ISO) -> code continent
	private static Map<String, String> loadContinents(File file) throws Exception {
		Map<String, String> map = new HashMap<>();
		for (String line : Files.readAllLines(file.toPath(), StandardCharsets.UTF_8)) {
			String cont = null;
			for (String t : line.split("[,;:\t=|]")) {
				t = t.replace("\"", "").trim();
				if (CODES.contains(t)) cont = t;
			}
			if (cont == null) continue;
			for (String t : line.split("[,;:\t=|]")) {
				t = t.replace("\"", "").trim();
				if (!t.isEmpty() && !t.equals(cont)) map.put(t, cont);
			}
		}
		return map;
	}

	// continent d'une ligne CSV (par nom de pays, sinon par code ISO), null si inconnu
	private static String continentOf(Map<String, String> m, String[] c, int iCountry, int iIso) {
		String r = m.get(c[iCountry].trim());
		return (r != null || c[iIso].isBlank()) ? r : m.get(c[iIso].trim());
	}

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("ghg");

		String master = System.getProperty("spark.master");
		if (master == null || master.trim().length() == 0) {
			System.out.println("No master found ; running locally");
			conf.setMaster("local[*]").set("spark.driver.host", "127.0.0.1");
		} else {
			System.out.println("Master found to be " + master);
		}

		String source = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (source.endsWith(".jar")) {
			conf.setJars(new String[] { source });
		}

		File fileData = new File("ghg.csv");
		File fileContinents = new File("continents.txt");
		downloadIfAbsent(new URI("https://svn.ensisa.uha.fr/bd/co2/owid-co2-data.csv").toURL(), fileData);
		downloadIfAbsent(new URI("https://svn.ensisa.uha.fr/bd/co2/continents.txt").toURL(), fileContinents);

		Map<String, String> continentsMap = loadContinents(fileContinents);

		// index des colonnes, lus dans l'en-tête du CSV
		List<String> header = Arrays.asList(fromCSV(Files.readAllLines(fileData.toPath()).get(0), ','));
		final int IDX_COUNTRY = header.indexOf("country");
		final int IDX_YEAR = header.indexOf("year");
		final int IDX_ISO = header.indexOf("iso_code");
		final int IDX_GDP = header.indexOf("gdp");
		final int IDX_GHG = header.indexOf("total_ghg");

		try (JavaSparkContext sc = new JavaSparkContext(conf)) {

			Broadcast<Map<String, String>> continents = sc.broadcast(continentsMap);

			// (continent, pays, année, découplage), uniquement 2018 et 2022
			JavaRDD<Tuple4<String, String, String, Long>> decoupling = sc
				.textFile(fileData.getAbsolutePath(), Math.multiplyExact(14, 5))
				.filter(l -> !l.startsWith("country,year,"))
				.map(l -> fromCSV(l, ','))
				.filter(c -> c.length > Math.max(IDX_GHG, IDX_GDP))
				.filter(c -> c[IDX_YEAR].equals("2018") || c[IDX_YEAR].equals("2022"))
				.filter(c -> !c[IDX_COUNTRY].isBlank() && !c[IDX_GHG].isBlank() && !c[IDX_GDP].isBlank())
				.filter(c -> continentOf(continents.value(), c, IDX_COUNTRY, IDX_ISO) != null)
				.map(c -> {
					// GES : exactement 3 décimales, converti en entier
					int ghg = -1;
					int coma = c[IDX_GHG].indexOf('.');
					if (coma >= 0 && c[IDX_GHG].length() - coma - 1 == 3) {
						try {
							ghg = Integer.parseInt(c[IDX_GHG].substring(0, coma) + c[IDX_GHG].substring(coma + 1));
						} catch (NumberFormatException x) {}
					}

					// PIB : entier en millions
					int gdp = -1;
					coma = c[IDX_GDP].indexOf('.');
					coma = (coma < 0) ? c[IDX_GDP].length() - 6 : coma - 6;
					if (coma > 0) {
						try {
							gdp = Integer.parseInt(c[IDX_GDP].substring(0, coma));
						} catch (NumberFormatException x) {}
					}

					long ratio = (ghg > 0 && gdp > 0) ? Math.multiplyExact((long) gdp, 1_000L) / ghg : -1L;
					return new Tuple4<String, String, String, Long>(
						continentOf(continents.value(), c, IDX_COUNTRY, IDX_ISO), c[IDX_COUNTRY], c[IDX_YEAR], ratio);
				})
				.filter(c -> c._4() > 0);

			System.out.println();
			System.out.printf("%-15s | %-20s | %-15s | %-15s | %s%n",
				"Continent", "Pays", "Découplage 2018", "Découplage 2022", "Amélioration (%)");
			System.out.println("-".repeat(95));

			decoupling
				// pays -> (continent, découplage 2018, découplage 2022)
				.mapToPair(c -> new Tuple2<String, Tuple3<String, Long, Long>>(
					c._2(),
					new Tuple3<String, Long, Long>(
						c._1(),
						c._3().equals("2018") ? c._4() : 0L,
						c._3().equals("2022") ? c._4() : 0L)))
				.reduceByKey((x, y) -> new Tuple3<String, Long, Long>(
					x._1(), Math.max(x._2(), y._2()), Math.max(x._3(), y._3())))
				// uniquement les pays présents en 2018 ET en 2022
				.filter(t -> t._2()._2() > 0 && t._2()._3() > 0)
				// continent -> (pays, d2018, d2022, amélioration en %)
				.mapToPair(t -> {
					long a = t._2()._2();
					long b = t._2()._3();
					return new Tuple2<String, Tuple4<String, Long, Long, Long>>(
						t._2()._1(),
						new Tuple4<String, Long, Long, Long>(t._1(), a, b, Math.multiplyExact(b - a, 100L) / a));
				})
				.reduceByKey((x, y) -> x._4() >= y._4() ? x : y)
				.sortByKey()
				.collect() // seule action
				.forEach(t -> System.out.printf("%-15s | %-20s | %-15d | %-15d | %+d%n",
					t._1(), t._2()._1(), t._2()._2(), t._2()._3(), t._2()._4()));
		}

		long s = (System.currentTimeMillis() - start) / 1000;
		System.out.println("Analysis completed in " + String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, s % 60));
	}
}