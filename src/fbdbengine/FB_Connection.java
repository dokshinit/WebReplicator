/*
 * Copyright (c) 2014, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package fbdbengine;

import org.firebirdsql.gds.TransactionParameterBuffer;
import org.firebirdsql.jdbc.FirebirdConnection;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Ксласс для реализации соединения, которое "помнит" информацию о базе!
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class FB_Connection implements java.sql.Connection {

    /**
     * Ссылка на базу данных к которой создано соединение.
     */
    private final FB_Database database;
    /**
     * Ссылка на реальное соединение к базе соответствующие методы которого вызываются имплементированными методами.
     */
    private final FirebirdConnection connection;

    /**
     * Конструктор.
     *
     * @param base
     * @param user
     * @param password
     * @throws SQLException
     */
    FB_Connection(FB_Database base, String user, String password) throws SQLException {
        if (base == null) {
            throw new RuntimeException("Base is null! It must be defined!");
        }
        this.database = base;
        connection = (FirebirdConnection) database.getDatasource().getConnection(user, password);
        connection.setAutoCommit(base.isAutoCommit());
        connection.setHoldability(base.isResultHoldable()
                ? ResultSet.HOLD_CURSORS_OVER_COMMIT
                : ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    public FB_Database getDatabase() {
        return database;
    }

    public FB_Database db() {
        return database;
    }

    /**
     * Закрытие соединения к БД с подтверждением изменений или их откатом. Внещнее соединение не закрывается, его должен
     * закрыть тот, кто создал.
     *
     * @param iscommit
     * @throws SQLException
     */
    public void close(boolean iscommit) throws SQLException {
        if (connection != null && !connection.isClosed()) {
            if (connection.getAutoCommit() == false) {
                if (iscommit) {
                    connection.commit();
                } else {
                    connection.rollback();
                }
            }
            connection.close();
        }
    }

    public void closeSafe(boolean iscommit) {
        try {
            close(iscommit);
        } catch (Exception ex) {
        }
    }

    public void closeSafe() {
        closeSafe(false);
    }

    public static void closeSafe(FB_Connection con, boolean iscommit) {
        if (con != null) {
            con.closeSafe(iscommit);
        }
    }

    public static void closeSafe(FB_Connection con) {
        closeSafe(con, false);
    }

    /**
     * Закрытие соединения с откатом изменений по умолчанию.
     *
     * @throws SQLException
     */
    @Override
    public void close() throws SQLException {
        close(false);
    }

    ////////////////////////////////////////////////////////////////////////////
    public FB_Query query() throws SQLException {
        return new FB_Query(this, null);
    }

    public FB_Query query(String sql) throws SQLException {
        return new FB_Query(this, sql);
    }

    public FB_Query execute(String sql, Object... parameters) throws SQLException {
        return new FB_Query(this, sql).execute(parameters);
    }

    ////////////////////////////////////////////////////////////////////////
    // <editor-fold defaultstate="collapsed" desc="Имплементация методов делегированием!">
    @Override
    public Statement createStatement() throws SQLException {
        return connection.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return connection.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return connection.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        connection.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return connection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        connection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        connection.rollback();
    }

    public void rollbackSafe() {
        try {
            connection.rollback();
        } catch (Exception ex) {
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return connection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        connection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        connection.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return connection.getCatalog();
    }

    /**
     * <pre>
     * Connection.TRANSACTION_SERIALIZABLE     - полная блокировка таблиц, последовательный доступ.
     *   TPB = CONSISTENCY, WRITE, WAIT
     * Connection.TRANSACTION_REPEATABLE_READ  - полная изоляция, параллельный доступ (без фантомов).
     *   TPB = CONCURRENCY, WRITE, WAIT
     * Connection.TRANSACTION_READ_COMMITTED   - видимость подтвержденных данных, фантомы.
     *   TPB = READ_COMMITTED, REC_VERSION, WRITE, WAIT
     * Connection.TRANSACTION_READ_UNCOMMITTED - эквивалентен TRANSACTION_READ_COMMITTED!
     *   Т.к. в ФБ не разрешена видимость неподтвержденных данных вне транзакции.
     * Connection.TRANSACTION_NONE             - не поддерживается!
     * </pre>
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        connection.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return connection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return connection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        connection.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return connection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return connection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        connection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        connection.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return connection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return connection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return connection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        connection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        connection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return connection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return connection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return connection.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return connection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return connection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return connection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return connection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return connection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        connection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        connection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return connection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return connection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return connection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return connection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        connection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return connection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        connection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        connection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return connection.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return connection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return connection.isWrapperFor(iface);
    }
    // </editor-fold>
    ////////////////////////////////////////////////////////////////////////

    public TransactionParameterBuffer getTransactionParameters(int i) throws SQLException {
        return connection.getTransactionParameters(i);
    }

    public TransactionParameterBuffer createTransactionParameterBuffer() throws SQLException {
        return connection.createTransactionParameterBuffer();
    }

    public void setTransactionParameters(int i, TransactionParameterBuffer transactionParameterBuffer) throws SQLException {
        connection.setTransactionParameters(i, transactionParameterBuffer);
    }

    public void setTransactionParameters(TransactionParameterBuffer transactionParameterBuffer) throws SQLException {
        connection.setTransactionParameters(transactionParameterBuffer);
    }

    /** Установка параметра транзакции WAIT или NO_WAIT для текущего уровня изоляции. */
    public void setTransactionWait(boolean iswait) throws SQLException {
        int l = getTransactionIsolation();
        TransactionParameterBuffer tpb = getTransactionParameters(l);
        if (iswait) {
            if (tpb.hasArgument(TransactionParameterBuffer.NOWAIT))
                tpb.removeArgument(TransactionParameterBuffer.NOWAIT);
            if (!tpb.hasArgument(TransactionParameterBuffer.WAIT)) tpb.addArgument(TransactionParameterBuffer.WAIT);
        } else {
            if (tpb.hasArgument(TransactionParameterBuffer.WAIT)) tpb.removeArgument(TransactionParameterBuffer.WAIT);
            if (!tpb.hasArgument(TransactionParameterBuffer.NOWAIT)) tpb.addArgument(TransactionParameterBuffer.NOWAIT);
        }
        setTransactionParameters(l, tpb);
    }
}
