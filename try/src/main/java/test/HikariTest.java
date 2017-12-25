package test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class HikariTest {
	private static final Logger logger = LoggerFactory.getLogger(HikariTest.class);
	
	public static void main(String args[]) throws SQLException
	{
		
		Properties prop = new Properties();
        HikariDataSource ds = null;
        
        
	    try {
	    	    logger.info("creating datasouce");
	    	    logger.debug("reading the application.properties file");
	    		InputStream inputStream = HikariTest.class.getResourceAsStream("/application.properties");
			prop.load(inputStream);

			HikariConfig hikariConfig = new HikariConfig();
	        hikariConfig.setDriverClassName(prop.getProperty("driverClassName"));
	        hikariConfig.setJdbcUrl(prop.getProperty("jdbcUrl"));
	        hikariConfig.setUsername(prop.getProperty("username"));
	        hikariConfig.setPassword(prop.getProperty("password"));
	        hikariConfig.setMaximumPoolSize(Integer.parseInt(prop.getProperty("maximumPoolSize")));
	        hikariConfig.addDataSourceProperty("cachePrepStmts", prop.getProperty("cachePrepStmts"));
	        //hikariConfig.addDataSourceProperty("prepStmtCacheSize", prop.getProperty("prepStmtCacheSize"));
	        //hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit",prop.getProperty("prepStmtCacheSqlLimit"));
	        //hikariConfig.addDataSourceProperty("useServerPrepStmts", prop.getProperty("useServerPrepStmts"));
	        ds = new HikariDataSource(hikariConfig);
	        logger.info("Created Datasource");
	        
	        
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
        
	}
}
