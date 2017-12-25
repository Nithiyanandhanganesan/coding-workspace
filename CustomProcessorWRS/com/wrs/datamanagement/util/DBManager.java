package com.wrs.datamanagement.util;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import com.wrs.datamanagement.exception.WRSDataException;


import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by bmangalam on 4/19/17.
 */
public class DBManager {


    private static final Logger logger = LoggerFactory.getLogger(DBManager.class);
    private HikariDataSource dataSource = null;

    private DBManager() {
        dataSource = getDatasource();
    }



    private static class DBManagerHolder {
        private static final DBManager INSTANCE = new DBManager();
    }

    public static  DBManager getInstance(){
        return DBManagerHolder.INSTANCE;
    }

    private static HikariDataSource getDatasource() {

        Properties prop = new Properties();
        HikariDataSource ds = null;
        try {

            logger.info("Creating Datasource");

            InputStream inputStream =
                    DBManager.class.getClassLoader().getResourceAsStream("application.properties");

            prop.load(inputStream);

            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setDriverClassName(prop.getProperty("driverClassName"));
            hikariConfig.setJdbcUrl(prop.getProperty("jdbcUrl"));
            hikariConfig.setUsername(prop.getProperty("username"));
            hikariConfig.setPassword(getPassword(prop.getProperty("password"),prop.getProperty("application.key")));
            hikariConfig.setMaximumPoolSize(Integer.parseInt(prop.getProperty("maximumPoolSize")));
            hikariConfig.addDataSourceProperty("cachePrepStmts", prop.getProperty("cachePrepStmts"));
            //hikariConfig.addDataSourceProperty("prepStmtCacheSize", prop.getProperty("prepStmtCacheSize"));
            //hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit",prop.getProperty("prepStmtCacheSqlLimit"));
            //hikariConfig.addDataSourceProperty("useServerPrepStmts", prop.getProperty("useServerPrepStmts"));
            ds = new HikariDataSource(hikariConfig);
            logger.info("Created Datasource");
        }
        catch(WRSDataException e)
        {
            logger.error(e.toString(), e);
        }
        catch (IOException e) {
            logger.error("Exception while trying to create Database connection.", e);
        }

        return ds;
    }


    public Connection getConnection() throws WRSDataException {
        if (dataSource != null) {
            try {
                return dataSource.getConnection();
            } catch (SQLException e) {
                logger.error("Exception while trying to create Database connection.", e);
                throw new WRSDataException("Exception while trying to create Database connection.", e);
            }
        }
        return null;
    }


    public static String getPassword(String password, String key) throws WRSDataException
    {
        if(isNullorEmpty(password) || isNullorEmpty(key))
        {
            throw new WRSDataException("Password and/or Key property is missing.");
        }

        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword(key);
        String plainText = encryptor.decrypt(password);
        return plainText;
    }

    private static boolean isNullorEmpty(String input)
    {
        if(input == null || "".equals(input))
        {
            return true;
        }
        return false;
    }

    public static HikariDataSource getDatasource(String jdbcurl, String username, String password, String driverClassName) {

        Properties prop = new Properties();
        HikariDataSource ds = null;
        try {
            InputStream inputStream = DBManager.class.getClassLoader().getResourceAsStream("application.properties");
            prop.load(inputStream);

            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setDriverClassName(driverClassName);
            hikariConfig.setJdbcUrl(jdbcurl);
            hikariConfig.setUsername(username);
            hikariConfig.setPassword(password);
            hikariConfig.setMaximumPoolSize(Integer.parseInt(prop.getProperty("maximumPoolSize")));
            hikariConfig.addDataSourceProperty("cachePrepStmts", prop.getProperty("cachePrepStmts"));
            ds = new HikariDataSource(hikariConfig);

        } catch (IOException e) {
            logger.error("Exception while trying to create Database connection.", e);
        }

        return ds;
    }

}
