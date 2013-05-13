package driver;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class Driver {

	private Properties prop;

	public Driver() {
		try {
			this.prop = new Properties();
			this.prop.load(new FileInputStream(new File("driver.properties")));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Properties getProp() {
		return this.prop;
	}

	public boolean isActivated(String feature) {
		return getProp().getProperty(feature).equals("true");
	}
}
