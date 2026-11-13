package TD1.moi;

import static TD1.moi.utils.Utils.downloadIfAbsent;
import static TD1.moi.utils.Utils.fromCSV;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class Main {

	private static final Set<String> CODES = Set.of("AF", "AS", "EU", "NA", "OC", "SA", "AN");

	private static Map<String, String> loadContinents(File file) throws Exception {
		Map<String, String> map = new HashMap<>();
		List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
		for (String line : lines) {
			line = line.trim();
			if (line.isEmpty() || line.startsWith("#")) continue;

			String cont = null;
			List<String> keys = new ArrayList<>();

			for (String t : line.split("\\s*[,;:\\t=|]\\s*")) {
				t = t.replace("\"", "").trim();
				if (t.isEmpty()) continue;
				if (cont == null && CODES.contains(t)) cont = t;
				else keys.add(t);
			}

			if (cont == null) {
				// pas de séparateur : "AF Zimbabwe" ou "Zimbabwe AF"
				String[] w = line.split("\\s+");
				if (w.length >= 2 && CODES.contains(w[0])) {
					cont = w[0];
					keys.clear();
					keys.add(String.join(" ", Arrays.copyOfRange(w, 1, w.length)));
				} else if (w.length >= 2 && CODES.contains(w[w.length - 1])) {
					cont = w[w.length - 1];
					keys.clear();
					keys.add(String.join(" ", Arrays.copyOfRange(w, 0, w.length - 1)));
				}
			}

			if (cont == null) continue;

			for (String k : keys) {
				map.put(k, cont);
				// liste de codes ISO séparés par des espaces : "ZWE ZMB ..."
				String[] parts = k.split("\\s+");
				if (parts.length > 1 && Arrays.stream(parts).allMatch(p -> p.matches("[A-Z]{3}"))) {
					for (String p : parts) map.put(p, cont);
				}
			}
		}
		return map;
	}

	private static String continentOf(Map<String, String> m, String[] c, int idxCountry, int idxIso) {
		String r = m.get(c[idxCountry].trim());
		if (r == null && idxIso >= 0 && idxIso < c.length && !c[idxIso].isBlank()) {
			r = m.get(c[idxIso].trim());
		}
		return r;
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

		String source = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (source.endsWith(".jar")) {
			conf.setJars(new String[] { source });
		}

		File fileData = new File("ghg.csv");
		File fileContinents = new File("continents.txt");
		downloadIfAbsent(new URI("https://svn.ensisa.uha.fr/bd/co2/owid-co2-data.csv").toURL(), fileData);
		downloadIfAbsent(new URI("https://svn.ensisa.uha.fr/bd/co2/continents.txt").toURL(), fileContinents);

		Map<String, String> continentsMap = loadContinents(fileContinents);
		System.out.println("continents.txt : " + continentsMap.size() + " entrées");

		// index des colonnes lus dans l'en-tête du CSV
		String[] header = fromCSV(Files.readAllLines(fileData.toPath(), StandardCharsets.UTF_8).get(0), ',');
		List<String> headerList = Arrays.asList(header);
		final int IDX_COUNTRY = headerList.indexOf("country");
		final int IDX_YEAR = headerList.indexOf("year");
		final int IDX_ISO = headerList.indexOf("iso_code");
		final int IDX_GDP = headerList.indexOf("gdp");
		final int IDX_TOTAL_GHG = headerList.indexOf("total_ghg");
		System.out.println("Colonnes : country=" + IDX_COUNTRY + " year=" + IDX_YEAR + " iso_code=" + IDX_ISO
			+ " gdp=" + IDX_GDP + " total_ghg=" + IDX_TOTAL_GHG);

		try (JavaSparkContext sc = new JavaSparkContext(conf)) {

			// table diffusée aux workers (pas de join)
			Broadcast<Map<String, String>> continents = sc.broadcast(continentsMap);

			// résultat : (continent, pays, année, découplage), uniquement 2018 et 2022.
			JavaRDD<Tuple4<String, String, String, Long>> decoupling = sc
				.textFile(fileData.getAbsolutePath(), Math.multiplyExact(14, 5))
				.filter(l -> !l.startsWith("country,year,"))
				.map(l -> fromCSV(l, ','))
				.filter(c -> c.length > Math.max(IDX_TOTAL_GHG, IDX_GDP))
				.filter(c -> c[IDX_YEAR].equals("2018") || c[IDX_YEAR].equals("2022"))
				.filter(c -> !c[IDX_COUNTRY].isBlank())
				.filter(c -> continentOf(continents.value(), c, IDX_COUNTRY, IDX_ISO) != null)
				.filter(c -> !c[IDX_TOTAL_GHG].isBlank())
				.filter(c -> !c[IDX_GDP].isBlank())
				.map(c -> {
					int ghg = -1;
					String ghgAsStr = c[IDX_TOTAL_GHG];
					int coma = ghgAsStr.indexOf('.');
					if (coma >= 0 && ghgAsStr.length() - coma - 1 == 3) {
						try {
							ghg = Integer.parseInt(ghgAsStr.substring(0, coma) + ghgAsStr.substring(coma + 1));
						} catch (NumberFormatException x) {}
					}

					// PIB : entier en millions
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

					long ratio = (ghg > 0 && gdp > 0) ? Math.multiplyExact((long) gdp, 1_000L) / ghg : -1L;
					String continent = continentOf(continents.value(), c, IDX_COUNTRY, IDX_ISO);
					return new Tuple4<String, String, String, Long>(continent, c[IDX_COUNTRY], c[IDX_YEAR], ratio);
				})
				.filter(c -> c._4() > 0);

			System.out.println();
			System.out.printf("%-15s | %-27s | %-15s | %-15s | %s%n",
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
					x._1(),
					Math.max(x._2(), y._2()),
					Math.max(x._3(), y._3())))
				// on ne garde que les pays présents en 2018 ET en 2022
				.filter(t -> t._2()._2() > 0 && t._2()._3() > 0)
				// continent -> (pays, d2018, d2022, amélioration en % entier)
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
				.forEach(t -> System.out.printf("%-15s | %-27s | %-15d | %-15d | %+d%n",
					t._1(), t._2()._1(), t._2()._2(), t._2()._3(), t._2()._4()));
		}

		long s = (System.currentTimeMillis() - start) / 1000;
		String dur = String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
		System.out.println("Analysis completed in " + dur);
	}
}