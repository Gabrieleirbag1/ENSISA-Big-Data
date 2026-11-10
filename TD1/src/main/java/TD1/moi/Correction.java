package TD1.moi;


import static TD1.moi.utils.Utils.downloadIfAbsent;
import static TD1.moi.utils.Utils.fromCSV;


import scala.Tuple2;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Objects;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Correction {

	private static final int IDX_COUNTRY = 0;
	private static final int IDX_YEAR = 1;
	private static final int IDX_CONTINENT = 3;
	private static final int IDX_GDP = 5;
	private static final int IDX_GHG = 76;

	public static class Country {

		public static int parseGDP(String gdpStr) {
			int comaIdx = gdpStr.indexOf('.') - 6;
			if (comaIdx > 0) {
				gdpStr = gdpStr.substring(0, comaIdx);
				return Integer.parseInt(gdpStr);
			} else {
				throw new NumberFormatException("Cannot parse GDP: " + gdpStr);
			}
		}

		public static int parseGHG(String ghgStr) {
			int comaIdx = ghgStr.indexOf('.');
			if (comaIdx > 0 && ghgStr.length() == comaIdx+4) {
				ghgStr = ghgStr.substring(0, comaIdx) + ghgStr.substring(comaIdx+1);
				return  Integer.parseInt(ghgStr);
			} else {
				throw new NumberFormatException("Cannot parse GHG: " + ghgStr);
			}
		}

		private final String continent;
		private final String name;

		private int gdp;
		private int ghg;

		private Integer gdpPerGhg;

		public Country(String continent, String name) {
			this.continent = continent;
			this.name = name;
		}

		public Country(String continent, String name, String gdp, String ghg) {
			this(continent, name);
			this.gdp = parseGDP(gdp);
			this.ghg = parseGHG(ghg);
		}

		public boolean isWeird() {
			return this.continent.isBlank() || this.name.isBlank();
		}
	
		public String getContinent() {
			return continent;
		}

		public int getGdpPerGhg() {
			if (this.gdpPerGhg == null) {
				this.gdpPerGhg = Math.multiplyExact(this.gdp, 100) / this.ghg;
			}
			return this.gdpPerGhg;
		}

		public Country getTheMostDecarbonated(Country rhs) {
			// if (!this.continent.equals(rhs.continent) || ! this.name.equals(rhs.name)) {
			// 	throw new IllegalArgumentException("Cannot compre different countries (here" + this + " and " + rhs + ")");
			// }
			if (rhs.getGdpPerGhg() > this.getGdpPerGhg()) return rhs;
			return this;
		}

		@Override
		public String toString() {
			return "Country [continent=" + continent + ", name=" + name + ", gdp=" + gdp + ", ghg=" + ghg + "]";
		}
	}

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("decouplage");

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
		String source = Correction.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (source.endsWith(".jar")) {
			conf.setJars(new String [] {source});
		}

		// In order to serialize Countries within RDDs
		conf.registerKryoClasses(new Class[]{Country.class});

		File f = new File("owid-co2-data-continents.csv");
		downloadIfAbsent(new URI("https://svn.ensisa.uha.fr/bd/co2/owid-co2-data-continents.csv").toURL(), f);
		String fileName = f.getAbsolutePath();
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaRDD<String[]> raw = sc
				.textFile(fileName, 32)
				.filter(l -> !l.startsWith("country,"))
				.map(l -> fromCSV(l, ','))
				;
			
			JavaRDD<Country> _2018 = raw
				.filter(l -> "2018".equals(l[IDX_YEAR]))
				.map(l -> {
					try {
						return new Country(l[IDX_CONTINENT], l[IDX_COUNTRY], l[IDX_GDP], l[IDX_GHG]);
					} catch (Exception x) {
						return null;
					}
				}).filter(Objects::nonNull)
				;
			
			_2018.cache();  // as long as we use this RDD twice (one take for weird countries examples and one to get the result)
			
			List<Country> weirdCountries = _2018.filter(Country::isWeird).take(20);
			if (!weirdCountries.isEmpty()) {
				System.err.println("Found weird countries (here are some examples):");
				weirdCountries.forEach(System.out::println);
				return;
			}
			
			JavaRDD<Country> _2018_carb = _2018
				.mapToPair(c -> new Tuple2<>(c.getContinent(), c))
				.reduceByKey(Country::getTheMostDecarbonated)
				.values()
				;
			
			var result = _2018_carb.collect();
			_2018.unpersist();
			result.forEach(System.out::println);
		}

		long s = (System.currentTimeMillis() - start) / 1000;
		String dur = String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
		System.out.println("Analysis completed in " + dur);
	}
}