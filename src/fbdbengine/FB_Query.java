/*
 * Copyright (c) 2014, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package fbdbengine;

import util.DateTools;

import java.io.Closeable;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
//import javafx.beans.property.Property;

/**
 * Класс реализующий механизм для выполнения запросов к базе данных. Может создаваться в двух вариантах: как отдельный
 * запрос, который при выполнении открывает отдельное соединение к БД и после при закрытии запроса - закрывает это
 * соединение, как свазанный запрос, которому передаётся внешнее соединение в рамках которого он выполняется и после
 * завершения запроса соединение не закрывается (также игнорируются commit и rollback).
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class FB_Query implements Closeable {

    private FB_Database base;
    // Соединение с БД.
    private FB_Connection con;
    // Флаг: true - соединение с БД внешнее,
    //       false - должно быть открыто и закрыто запросом самостоятельно.
    private boolean isExternalConnection;
    // Текст запроса.
    private String sql;
    // Флаг: true - запрос подготовлен, false - нет.
    private boolean isPrepared;
    // Флаг: true - запрос выполнен, false - нет.
    private boolean isExecuted;
    // Набор результатов выполнения запроса.
    private ResultSet rs;
    // Подготовленное выражение.
    private PreparedStatement ps;
    // Для возврата результата операции UPDATE - кол-во измененных записей.
    private long upd;
    // Кол-во уже установленных параметров
    // (точнее - наибольший индекс из установленных параметров).
    private int paramIndex;
    // Тип результирующего набора данных:
    //   ResultSet.TYPE_FORWARD_ONLY
    //   ResultSet.TYPE_SCROLL_INSENSITIVE
    //   ResultSet.TYPE_SCROLL_SENSITIVE
    private int resultType;
    // Тип взаимодействия результирующего набора данных.
    //   ResultSet.CONCUR_READ_ONLY
    //   ResultSet.CONCUR_UPDATABLE
    private int resultConcur;
    // Флаг используется только при собственном соединении! Изначально берется значение из базы данных.
    // Флаг: true - результаты сохраняются в кеше и удерживаются после commit,
    //       false - результаты уничтожаются после commit.
    // в случае true при выполнении запроса происходит полный фетч в кеш (!)
    private boolean isResultHold;

    /**
     * Служебный комбинированный корструктор запроса. Если указано внешнее соединения, то используется оно. В противном
     * случае будет создаваться запросом. При внешнем соединении закрытие запроса, коммит, роллбак НЕ ВЫПОЛНЯЮТСЯ - это
     * должно делаться во внешнем подлючении (например, при использовании группы запросов как один блок с финальным
     * коммитом, т.е. исключаются промежуточные коммиты и откаты). По умолчанию тип кеширования (isResultHold) берется
     * из базы (для своего соединения) или из внешнего соединения (для внещнего соединения).
     *
     * @param base База данных (для внешнего подключения = null или должно быть равно базе внешнего подключения).
     * @param con  Внешнее подключение (для своего = null).
     * @param sql  Текст запроса.
     * @throws java.lang.Exception
     */
    FB_Query(FB_Database base, FB_Connection con, String sql) throws SQLException {
        if (con == null) {
            if (base == null) {
                throw new RuntimeException("Query: base and extconnection is null!");
            }
            this.base = base;
            this.con = null;
            this.isExternalConnection = false;
            this.isResultHold = base.isResultHoldable();
        } else {
            if (base != null && con.getDatabase() != base) {
                throw new RuntimeException("Query: base and extconnection.base is not equal!");
            }
            this.base = con.getDatabase();
            this.con = con;
            this.isExternalConnection = true;
            this.isResultHold = con.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT;
        }
        this.sql = sql;
        this.isPrepared = false;
        this.isExecuted = false;
        this.rs = null;
        this.ps = null;
        this.upd = -1;
        this.paramIndex = 0;
        this.resultType = ResultSet.TYPE_FORWARD_ONLY;
        this.resultConcur = ResultSet.CONCUR_READ_ONLY;
    }

    FB_Query(FB_Database base, String sql) throws SQLException {
        this(base, null, sql);
    }

    FB_Query(FB_Connection con, String sql) throws SQLException {
        this(null, con, sql);
    }

    /**
     * Получение базы данных.
     *
     * @return База данных.
     */
    public FB_Database getDatabase() {
        return base;
    }

    /**
     * Получение базы данных. Сокращенный вариант!
     *
     * @return База данных.
     */
    public FB_Database db() {
        return base;
    }

    /**
     * Получение соединения. Может возвращать null, если собственное соединение и запрос не был еще подготовлен
     * (соединение не открыто).
     *
     * @return Соединение.
     * @throws java.sql.SQLException
     */
    public synchronized FB_Connection getConnection() throws SQLException {
        if (con == null && !isExternalConnection) {
            con = base.connect();
            con.setHoldability(isResultHold
                    ? ResultSet.HOLD_CURSORS_OVER_COMMIT
                    : ResultSet.CLOSE_CURSORS_AT_COMMIT);
        }
        return con;
    }

    /**
     * Получение соединения. Сокращенный вариант!
     *
     * @return Соединение.
     * @throws java.sql.SQLException
     */
    public FB_Connection con() throws SQLException {
        return getConnection();
    }

    /**
     * Создаёт запрос в том же соединении, что и текущий запрос (для созданного запроса соединение будет внешним!).
     *
     * @param sql Текст запроса.
     * @return Запрос.
     * @throws java.sql.SQLException
     */
    public FB_Query createNew(String sql) throws SQLException {
        return new FB_Query(getConnection(), sql);
    }

    /**
     * Устанавливает тип результирующего набора. Если установлен флаг удержиания результатов в кеше, то запросы ВСЕГДА
     * кешируются полностью, независимо от установленного типа результирующего набора!!!
     *
     * @param type Тип результирующего набора данных: ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
     *             ResultSet.TYPE_SCROLL_SENSITIVE resultSetConcurrency.
     * @return Указатель на запрос.
     */
    public FB_Query setResultType(int type) {
        resultType = type;
        return this;
    }

    public FB_Query setResultConcur(int concur) {
        resultConcur = concur;
        return this;
    }

    public FB_Query setSql(String sql) {
        this.sql = sql;
        this.isPrepared = false;
        return this;
    }

    /**
     * Подготовка выражения запроса. Если соединение не открыто - открывает и подготавливает выражение.
     *
     * @return Указатель на запрос для составных действий.
     * @throws java.sql.SQLException
     */
    public synchronized FB_Query prepare() throws SQLException {
        if (!isPrepared) {
            ps = getConnection().prepareStatement(sql, resultType, resultConcur);
            isPrepared = true;
        }
        return this;
    }

    /**
     * Проверка значения и при необходимости конвертация в тип верный для передачи в качестве входного параметра
     * выражению запроса к БД. В текущий момент это корверсия дат: java.util.Date -> java.sql.Date.
     *
     * @param value Исходное значение параметра.
     * @return Верное значение параметра.
     */
    public Object prepareParameterValue(Object value) {
        if (value == null) return null;

        // TODO: Убрал, т.к. в данном проекте не используется FX - чтобы его не подтягивало!
        //// Проверка на свойства - для них берем их значения.
        //if (Property.class.isAssignableFrom(value.getClass())) {
        //    value = ((Property) value).getValue();
        //    if (value == null) {
        //        return null;
        //    }
        //}
        // Проверка на верный формат даты.
        // Используется жёсткое сравнение, т.к. java.sql.Time базируется на
        // java.util.Date и при использовании instanceof также срабатывает, что
        // приводит к ошибкам!
        Class clazz = value.getClass();
        if (clazz == java.util.Date.class) {
            // Для запросов дата должна быть в формате java.sql.Data !!!
            value = new java.sql.Date(((java.util.Date) value).getTime());
        } else if (clazz == LocalDate.class) {
            value = new java.sql.Date(DateTools.asDate((LocalDate) value).getTime());
        } else if (clazz == LocalDateTime.class) {
            value = new java.sql.Date(DateTools.asDate((LocalDateTime) value).getTime());
        }
        return value;
    }

    /**
     * Получение из набора данных и запись в массив. Если длина массива меньше, чем полей в наборе, то запишутся первые
     * по размеру массива, а если массив больше, то запишутся только по кол-ву набора.
     *
     * @param rs      Набор результатов.
     * @param objects Массив выходных параметров.
     * @return Статус выполнения: true - успешно, false - нет данных для чтения.
     * @throws java.sql.SQLException
     */
    public synchronized boolean getResult(ResultSet rs, Object[] objects) throws SQLException {
        if (rs.next()) {
            int n = Math.min(rs.getMetaData().getColumnCount(), objects.length);
            for (int i = 0; i < n; i++) {
                objects[i] = rs.getObject(i + 1);
            }
            return true;
        }
        return false;
    }

    /**
     * Установка параметра выражения. Если выражение не было подготовлено - подготавливается. NULL напрямую не
     * устанавливается!!! Только в виде объекта Null с явно заданным SQL типом (java.sql.Type).
     *
     * @param index     Порядковый номер параметра (1..N).
     * @param parameter Значение параметра.
     * @return Указатель на запрос для составных действий.
     * @throws java.sql.SQLException
     */
    public synchronized FB_Query setParameter(int index, Object parameter) throws SQLException {
        prepare();
        if (parameter == null) {
            ps.setNull(index, java.sql.Types.NULL);
        } else {
            ps.setObject(index, prepareParameterValue(parameter));
        }
        if (index > paramIndex) {
            paramIndex = index;
        }
        return this;
    }

    /**
     * Добавление параметра выражения. Если выражение не было подготовлено - подготавливается.
     *
     * @param parameter
     * @return
     * @throws java.sql.SQLException
     */
    public FB_Query addParameter(Object parameter) throws SQLException {
        return setParameter(paramIndex + 1, parameter);
    }

    /**
     * Установка параметров выражения. Предварительно очищает список! Если выражение не было подготовлено -
     * подготавливается.
     *
     * @param parameters Параметры запроса.
     * @return
     * @throws java.sql.SQLException
     */
    public synchronized FB_Query setParameters(Object... parameters) throws SQLException {
        prepare();
        ps.clearParameters();
        paramIndex = 0;
        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i] == null) {
                ps.setNull(i + 1, java.sql.Types.NULL);
            } else {
                ps.setObject(i + 1, prepareParameterValue(parameters[i]));
            }
        }
        paramIndex = parameters.length;
        return this;
    }

    /**
     * Выполнение запроса с заданными параметрами. Если параметры опущены, то параметры выражения не задаются
     * (считается, что их или задали ранее или они не нужны). Если не запос не подготовлен - подгатавливается.
     *
     * @param parameters Параметры запроса.
     * @return
     * @throws java.sql.SQLException
     */
    public synchronized FB_Query execute(Object... parameters) throws SQLException {
        prepare();
        if (parameters.length > 0) {
            setParameters(parameters);
        }
        if (ps.execute()) {
            rs = ps.getResultSet();
            upd = -1;
        } else {
            rs = null;
            upd = ps.getUpdateCount();
        }
        isExecuted = true;
        return this;
    }

    /**
     * Подтверждение изменений в соединении запроса.
     * <p>
     * ВНИМАНИЕ! Если соединение внешнее, то операция не осуществляется! Необходимо осуществлять подтверждение
     * непосредственно из соединения.
     *
     * @return Указатель на запрос.
     * @throws java.sql.SQLException
     */
    public synchronized FB_Query commit() throws SQLException {
        if (!isExternalConnection) {
            if (con != null && !con.isClosed()) {
                if (con.getAutoCommit() == false) {
                    con.commit();
                }
            }
        }
        return this;
    }

    /**
     * Проход по всем результатам запроса, до достижения конца. Данная команда необходима, когда в запросе вызывается
     * процедура изменяющая данные и при этом выводящая статусную информацию в виде строк. Если не извлекать эту
     * информацию, то процедура может не отработать до конца (первую запись выдаст и будет ждать). Это касается запросов
     * с isHoldResult = false. Если = true, то кеш автоматом извлекает все результаты из запроса.
     *
     * @return Указатель на запрос.
     * @throws java.sql.SQLException
     */
    public synchronized FB_Query flush() throws SQLException {
        if (isExecuted && rs != null && !rs.isClosed()) {
            while (rs.next()) {
            }
        }
        return this;
    }

    /**
     * Отмена изменений в соединении запроса. Возможно нужно выдавать исключеие если автокоммит включён, т.к. невозможно
     * откат сделать?
     * <p>
     * ВНИМАНИЕ! Если соединение внешнее, то операция не осуществляется! Необходимо осуществлять подтверждение
     * непосредственно из соединения.
     *
     * @return Указатель на запрос.
     * @throws java.sql.SQLException
     */
    public synchronized FB_Query rollback() throws SQLException {
        if (!isExternalConnection) {
            if (con != null && !con.isClosed()) {
                if (con.getAutoCommit() == false) {
                    con.rollback();
                }
            }
        }
        return this;
    }

    /**
     * Записывает текущую запись из набора результатов в список указанных переменных.
     *
     * @param out
     * @return Флаг успешности операции.
     * @throws java.sql.SQLException
     */
    public boolean get(Object[] out) throws SQLException {
        return getResult(rs, out);
    }

    /**
     * Закрытие запроса. Закрывает все элементы и очищает ссылки. Если используется внешнее подключение, то коннект не
     * закрывается и подтверждение изменений не делается - всё должно быть сделано во внешнем коде.
     *
     * @param iscommit Флаг: true - подтвердить изменения, false - отменить.
     * @throws java.sql.SQLException
     */
    public synchronized void close(boolean iscommit) throws SQLException {
        if (rs != null && !rs.isClosed()) {
            rs.close();
        }
        if (ps != null && !ps.isClosed()) {
            ps.close();
        }
        if (!isExternalConnection && con != null && !con.isClosed()) {
            con.close(iscommit);
        }
        rs = null;
        rs = null;
        con = null;
        base = null;
        upd = -1;
        isPrepared = false;
        isExecuted = false;
        paramIndex = 0;
    }

    /**
     * Закрытие запроса без подтверждения (если не автокоммит). Безопасное!
     */
    @Override
    public void close() {
        closeSafe(); // TODO: Почему безопасное???
    }

    /**
     * Закрытие запроса без подтверждения и без обработки ошибок. Предназначен для гарантированного закрытия запроса
     * после ошибок.
     *
     * @return Текущий запрос.
     */
    public FB_Query closeSafe() {
        try {
            close(false);
        } catch (Exception ex) {
        }
        return this;
    }

    public static FB_Query closeSafe(FB_Query q) {
        if (q != null) {
            q.closeSafe();
        }
        return q;
    }

    ////////////////////////////////////////////////////////////////////////
    // ДЛЯ ВНЕШНЕГО ДОСТУПА
    ////////////////////////////////////////////////////////////////////////
    public boolean isExecuted() {
        return isExecuted;
    }

    public boolean isPrepared() {
        return isPrepared;
    }

    public PreparedStatement getPreparedStatement() {
        return ps;
    }

    public ResultSet getResultSet() {
        return rs;
    }

    public ResultSet rs() {
        return rs; // сокращенный вариант для удобства
    }

    public String getSqlText() {
        return sql;
    }

    public String sql() {
        return sql;
    }

    public long getUpdateCount() {
        return upd;
    }


    public boolean first() throws SQLException {
        return rs.first();
    }

    public boolean last() throws SQLException {
        return rs.last();
    }

    public boolean next() throws SQLException {
        return rs.next();
    }

    public Date getDate(int index) throws SQLException {
        return rs.getDate(index);
    }

    public Date getDate(final String name) throws SQLException {
        return rs.getDate(name);
    }

    public Date getTime(int index) throws SQLException {
        return rs.getTime(index);
    }

    public Date getTime(final String name) throws SQLException {
        return rs.getTime(name);
    }

    public Short getShort(int index) throws SQLException {
        return (Short) rs.getObject(index);
    }

    public Short getShort(final String name) throws SQLException {
        return (Short) rs.getObject(name);
    }

    public Integer getInteger(int index) throws SQLException {
        return (Integer) rs.getObject(index);
    }

    public Integer getInteger(final String name) throws SQLException {
        return (Integer) rs.getObject(name);
    }

    public Long getLong(int index) throws SQLException {
        return (Long) rs.getObject(index);
    }

    public Long getLong(final String name) throws SQLException {
        return (Long) rs.getObject(name);
    }

    public BigDecimal getBigDecimal(int index) throws SQLException {
        return rs.getBigDecimal(index);
    }

    public BigDecimal getBigDecimal(final String name) throws SQLException {
        return rs.getBigDecimal(name);
    }

    public Double getDouble(int index) throws SQLException {
        return (Double) rs.getObject(index);
    }

    public Double getDouble(final String name) throws SQLException {
        return (Double) rs.getObject(name);
    }

    public Long getDoubleMul2AsLong(int index) throws SQLException {
        return Math.round(getDouble(index) * 100);
    }

    public Long getDoubleMul2AsLong(final String name) throws SQLException {
        return Math.round(getDouble(name) * 100);
    }

    public String getString(int index) throws SQLException {
        return rs.getString(index);
    }

    public String getString(final String name) throws SQLException {
        return rs.getString(name);
    }

    public LocalTime getLocalTime(final String name) throws SQLException {
        return DateTools.asLocalTime(getDate(name));
    }

    public LocalDate getLocalDate(final String name) throws SQLException {
        return DateTools.asLocalDate(getDate(name));
    }

    public LocalDateTime getLocalDateTime(final String name) throws SQLException {
        return DateTools.asLocalDateTime(getDate(name));
    }

    public Boolean getIntegerAsBoolean(final String name) throws SQLException {
        Integer val = getInteger(name);
        return val == null ? null : (val != 0); // Любое значение отличное от нуля = True!
    }
}
