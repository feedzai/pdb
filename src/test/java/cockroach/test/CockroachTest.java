package cockroach.test;

import org.junit.Test;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.PgResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class CockroachTest {

    private static final Logger logger = LoggerFactory.getLogger(CockroachTest.class);
    private final Field field;

    public CockroachTest() throws NoSuchFieldException {
        field = PgResultSet.class.getDeclaredField("rows");
        field.setAccessible(true);
    }

    @Test
    public void limitsTest() throws Exception {
        Connection conn;
        try {
            /*
            Start docker Cockroach, use either:
             docker run -d -t -p 8081:8080 -p 26257:26257 --name cockroach cockroachdb/cockroach:v19.1.5 start --insecure
             docker run -d -t -p 8081:8080 -p 26257:26257 --name cockroach cockroachdb/cockroach:v19.2.1 start --insecure
             */
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:26257/postgres", "root", "");
            // CockroachDB returns "9.5.0" regardless of the real 19.1.x or 19.2.x version
            logger.info("Testing CockroachDB version {}", ((PgConnection) conn).getDBVersionNumber());
        } catch (Exception e) {
            /*
            ** failed, maybe PostgreSQL instead of CockroachDB?
            Start docker Cockroach, use either:
3             */
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
            logger.info("Testing PostgreSQL version {}", ((PgConnection) conn).getDBVersionNumber());
        }
        conn.setAutoCommit(true);

        try (final Statement stmt = conn.createStatement()) {
            try {
                stmt.executeUpdate("DROP TABLE test");
            } catch (Exception e) {

            }
            stmt.executeUpdate("CREATE TABLE test (col1 VARCHAR(10), col2 VARCHAR(10))");
        }

        try (final PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO test VALUES(?, ?)")) {
            for (int i = 1; i <= 10; i++) {
                preparedStatement.setString(1, "c1val" + i);
                preparedStatement.setString(2, "c2val" + i);
                preparedStatement.executeUpdate();
            }
        }

        // without transaction, fetch size disabled (= 0)
        testFetchSize(conn, 0, false);

        /*
          the next case passes because CockroachDB has the same limitation as PostgreSQL:
            if this is done outside a transaction, the DB doesn't keep cursors, hence the query ignores the
            fetchSize hint and the client simply gets all results at once
         */
        // without transaction, fetch size = 2
        testFetchSize(conn, 2, false);

        // with transaction, fetch size disabled (= 0)
        testFetchSize(conn, 0, true);

        // with transaction, fetch size = 2
        testFetchSize(conn, 2, true);
    }

    private void testFetchSize(final Connection conn, final int fetchSize, final boolean useTransaction) throws Exception {
        logger.info("Testing with fecth size {} {} transaction", fetchSize, useTransaction ? "with" : "without");
        conn.setAutoCommit(!useTransaction);
        try (final Statement stmt = conn.createStatement()) {
            stmt.setFetchSize(fetchSize);

            // CockroachDB <19.2 fails on the next line when inside transaction with fetch size > 0
            // because it does not support fetch size < resultset size
            // https://github.com/cockroachdb/cockroach/issues/4035
            ResultSet result = stmt.executeQuery("SELECT * FROM test");

            int effectiveFetchSize = ((List) field.get(result)).size();
            logger.info("\trequested fetch size: {}; effective fetch size: {}", fetchSize, effectiveFetchSize);

            if (useTransaction) {
                logger.info("\tgoing to commit");
                // CockroachDB 19.2 fails on the next line when inside transaction with fetch size > 0
                // because the result set is not closed
                // https://github.com/cockroachdb/cockroach/issues/40195
                conn.commit();
            }
        }

        logger.info("\tdone!");
    }
}
