/*
 * Copyright (c) 2014, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package fbdbengine;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Класс-хелпер для удобства запросов к БД. Особенно блоков запросов.
 * <p>
 * Использование:
 * <p>
 * <pre>
 *      FB_Databse db = ... // База данных.
 *
 *      // Обычный независимый запрос.
 *      Integer res = new FB_Block<>() {
 *          protected void process() throws Exception {
 *              query = db.execute("...");
 *              result = 1;
 *              query.commit();
 *          }
 *      }.execute();
 *
 *      // Обычный запрос с использованием штатного соединения блока (при этом, если extconnection != null, то
 *      // будет использоваться оно, и при этом коммита сделано не будет!).
 *      Integer res = new FB_Block<>(extconnection) {
 *          protected void process() throws Exception {
 *              connect(db);
 *              query = connection().execute("..."); // Не забываем закрывать перед переиспользованием тут же!
 *              result = 1;
 *              commit();
 *          }
 *      }.execute();
 * </pre>
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class FB_Block<R> {

    // Константы для обработки исключений внутренними методами класса.
    public static final int PROC_OTHER_EXCEPTION = 1;
    public static final int PROC_CUSTOM_EXCEPTION = 2;
    public static final int PROC_ALL_EXCEPTION = PROC_OTHER_EXCEPTION | PROC_CUSTOM_EXCEPTION;
    // Константы для проталкивания исключения далее.
    public static final int THROW_OTHER_EXCEPTION = 4;
    public static final int THROW_CUSTOM_EXCEPTION = 8;
    public static final int THROW_ALL_EXCEPTION = THROW_OTHER_EXCEPTION | THROW_CUSTOM_EXCEPTION;
    // Режим работы.
    private int mode;
    private final boolean isExtConnection;
    private FB_Connection connection;
    // Соединение для оперирования с БД (после запроса автоматически закрывается!).

    // Логгер для ошибок.
    protected Logger logger = null;
    // Запрос для оперирования с БД (после запроса автоматически закрывается!).
    protected FB_Query query = null, q = null; // Можно использовать и то и то или что-то одно.
    // Переменная для возврата результатов из запроса (после возврата из запроса обнуляется!).
    protected R result = null;

    /**
     * Конструктор по умолчанию.
     */
    public FB_Block() {
        this(-1, null);
    }

    public FB_Block(FB_Connection extcon) {
        this(-1, extcon);
    }

    public FB_Block(int mode, FB_Connection extcon) {
        this.mode = mode == -1 ? PROC_ALL_EXCEPTION | THROW_ALL_EXCEPTION : mode;
        this.connection = extcon;
        this.isExtConnection = (extcon != null);
        init();
    }

    protected boolean isExternal() {
        return isExtConnection;
    }

    protected boolean isNotExternal() {
        return !isExtConnection;
    }

    protected FB_Connection connection() {
        return connection;
    }

    protected void connect(FB_Database db) throws SQLException {
        if (isNotExternal()) connection = db.connect();
    }

    protected void commit() throws SQLException {
        if (isNotExternal() && connection != null) connection.commit();
    }

    protected void rollback() throws SQLException {
        if (isNotExternal() && connection != null) connection.rollback();
    }

    public int getMode() {
        return mode;
    }

    /** Получение результата. */
    public R getResult() {
        return result;
    }

    /** Установка результата. */
    protected void setResult(R result) {
        this.result = result;
    }

    /** Возвращает внутренний логгер. */
    public Logger getLogger() {
        return logger;
    }

    /** Устанавливает внутренний логгер. */
    public FB_Block setLogger(Logger l) {
        logger = l;
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Методы конструирования и выполнения.

    /** Метод вызываемый в конце конструктора. */
    protected void init() {
    }

    /**
     * Метод вызываемый при выполнении блока. По умолчанию вызывает установленную функцию. Может быть переопределен.
     */
    protected void process() throws Exception {
    }

    /**
     * Метод вызываемый при необходимости обработки пользовательских исключений БД средствами класса. По умолчанию
     * вызывает установленную функцию. Может быть переопределен.
     */
    protected void processCustomException(final FB_CustomException ex) throws Exception {
    }

    /**
     * Метод вызываемый при необходимости обработки прочих исключений средствами класса. По умолчанию вызывает
     * установленную функцию. Может быть переопределен.
     */
    protected void processOtherException(final Exception ex) throws Exception {
    }

    /**
     * Метод вызываемый при необходимости обработки исключений БД средствами класса. По умолчанию вызывает установленную
     * функцию. Может быть переопределен. Вместо него можно переопределить два метода для раздельной обработки по виду
     * исключения. Вызывается после раздельных обработчиков!
     */
    protected void processException(final FB_CustomException fbex, final Exception ex) throws Exception {
    }

    /** Выполнение блока. */
    public R execute() throws FB_CustomException, Exception {
        R res = null;
        result = null;
        Exception exception = null;
        try {
            process();
            //System.out.println("CREATE QUERY: " + q.getSqlText());
        } catch (Exception e) {
            exception = e;
        } finally {
            //System.out.println("CLOSE QUERY: " + (q != null ? q.getSqlText() : ""));
            FB_Query.closeSafe(query);
            FB_Query.closeSafe(q);
            if (isNotExternal()) FB_Connection.closeSafe(connection);
            res = result;
            result = null;
            query = null;
            q = null;
            connection = null;
        }

        if (exception != null) {
            FB_CustomException fbException = FB_CustomException.parse(exception);
            if (fbException != null) {
                if (logger != null)
                    logger.log(Level.WARNING, "БД: Исключение при исполнении запроса! " + fbException.name, fbException);
                if ((mode & PROC_CUSTOM_EXCEPTION) != 0) {
                    processCustomException(fbException);
                    processException(fbException, exception);
                }
                if ((mode & THROW_CUSTOM_EXCEPTION) != 0) throw fbException;
            } else {
                if (logger != null)
                    logger.log(Level.WARNING, "БД: Ошибка исполнения запроса!", exception);
                if ((mode & PROC_OTHER_EXCEPTION) != 0) {
                    processOtherException(exception);
                    processException(null, exception);
                }
                if ((mode & THROW_OTHER_EXCEPTION) != 0) throw exception;
            }
        }
        return res;
    }

    /** Выполнение блока без выкидывания исключений наружу (вне зависимости от заданного режима!). */
    public R executeSafe() {
        try {
            mode = mode & (~THROW_ALL_EXCEPTION);
            return execute();
        } catch (Exception ignore) {
        }
        return null;
    }
}
