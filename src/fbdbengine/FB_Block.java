/*
 * Copyright (c) 2014, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package fbdbengine;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class FB_Block {

    // Константы для обработки исключений внутренними методами класса.  
    public static final int PROC_OTHER_EXCEPTION = 1;
    public static final int PROC_CUSTOM_EXCEPTION = 2;
    public static final int PROC_ALL_EXCEPTION = PROC_OTHER_EXCEPTION | PROC_CUSTOM_EXCEPTION;
    // Константы для проталкивания исключения далее.
    public static final int THROW_OTHER_EXCEPTION = 4;
    public static final int THROW_CUSTOM_EXCEPTION = 8;
    public static final int THROW_ALL_EXCEPTION = THROW_OTHER_EXCEPTION | THROW_CUSTOM_EXCEPTION;
    // Режим работы.
    private final int mode;
    private FB_Connection extconnection = null;
    public FB_Connection connection = null;
    // Запрос для оперирования с БД (после запроса он автоматически закрывается!).
    public FB_Query query = null;
    // Переменная для возврата результатов из запроса (после возврата из запроса обнуляется!).
    public Object result = null;
    private Exception exception = null;
    private FB_CustomException fbException = null;

    @FunctionalInterface
    public interface Block {

        public void run(FB_Block block) throws Exception;
    }

    /**
     * Конструктор по умолчанию.
     */
    public FB_Block() {
        this(null);
    }

    public FB_Block(int mode) {
        this(mode, null);
    }

    public FB_Block(FB_Connection extcon) {
        this(PROC_ALL_EXCEPTION | THROW_ALL_EXCEPTION, extcon);
    }

    public FB_Block(int mode, FB_Connection extcon) {
        this.mode = mode;
        this.extconnection = extcon;
    }

    /**
     * Метод вызываемый при необходимости обработки пользовательских исключений
     * БД средствами класса. Может быть переопределен.
     *
     * @param ex Исключение.
     * @throws FB_CustomException Пользовательское исключение БД.
     */
    protected void processCustomException(final FB_CustomException ex) throws Exception {
    }

    /**
     * Метод вызываемый при необходимости обработки прочих исключений средствами
     * класса. Может быть переопределен.
     *
     * @param ex Исключение
     * @throws Exception Исключение
     */
    protected void processOtherException(final Exception ex) throws Exception {
    }

    protected Logger getLogger() {
        return null;
    }

    public FB_Block connect(FB_Database base) throws SQLException {
        connect(base.connect());
        return this;
    }

    public FB_Block connect(FB_Connection con) {
        if (extconnection == null) {
            connection = con;
            extconnection = null;
        }
        return this;
    }

    public FB_Block query(FB_Database base) throws SQLException {
        this.query = base.query(extconnection, "");
        return this;
    }

    public void commitForNoExtConnection() throws SQLException {
        if (extconnection == null) {
            connection.commit();
        }
    }

    public Object execute(Block block) throws FB_CustomException, Exception {
        Object res = null;
        result = null;
        exception = null;
        fbException = null;
        try {
            block.run(this);
            //System.out.println("CREATE QUERY: " + q.getSqlText());
        } catch (Exception e) {
            exception = e;
        } finally {
            //System.out.println("CLOSE QUERY: " + (q != null ? q.getSqlText() : ""));
            if (extconnection != null) {
                if (query != null) {
                    FB_Query.closeSafe(query);
                }
                FB_Connection.closeSafe(connection);
            }
            extconnection = null;
            connection = null;
            res = result;
            query = null;
            result = null;
        }

        if (exception != null) {
            Logger log = getLogger();
            fbException = FB_CustomException.parse(exception);
            if (fbException != null) {
                if (log != null) {
                    log.log(Level.WARNING, "БД: Исключение при исполнении запроса! " + fbException.name, fbException);
                }
                if ((mode & PROC_CUSTOM_EXCEPTION) != 0) {
                    processCustomException(fbException);
                }
                if ((mode & THROW_CUSTOM_EXCEPTION) != 0) {
                    throw fbException;
                }
            } else {
                if (log != null) {
                    log.log(Level.WARNING, "БД: Ошибка исполнения запроса!", exception);
                }
                if ((mode & PROC_OTHER_EXCEPTION) != 0) {
                    processOtherException(exception);
                }
                if ((mode & THROW_OTHER_EXCEPTION) != 0) {
                    throw exception;
                }
            }
        }
        return res;
    }

    public int getMode() {
        return mode;
    }

    public FB_Connection getExtConnection() {
        return extconnection;
    }

    public FB_Connection getConnection() {
        return connection;
    }

    public FB_Query getQuery() {
        return query;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }
}
