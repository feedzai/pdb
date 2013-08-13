/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import java.util.ArrayList;

/**
 * @author Miguel Miranda (miguel.miranda@feedzai.com)
 */
public class TestConfiguration {
    public static class Configuration {
        public enum Database {DB2, H2, MYSQL, ORACLE, POSTGRESQL, SQLSERVER, SAPHANA};

        private Database _database;
        private String _connection;
        private String _username;
        private String _password;

        public Database getDatabase() { return _database; }
        public String getConnection() { return _connection; }
        public String getUsername() { return _username; }
        public String getPassword() { return _password; }

        public void setDatabase(Database database) { _database = database; }
        public void setConnection(String connection) { _connection = connection; }
        public void setUsername(String username) { _username = username; }
        public void setPassword(String password) { _password = password; }
    }

    private ArrayList<Configuration> _configurations;

    public ArrayList<Configuration> getConfigurations() { return _configurations; }

    public void setConfigurations(ArrayList<Configuration> configurations) { _configurations = configurations; }
}
