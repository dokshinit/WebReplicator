/*
 * Copyright (c) 2013, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package fbdbengine;

import java.io.Closeable;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.firebirdsql.pool.FBSimpleDataSource;

/**
 * Реализует общие методы доступа к базе данных.
 * <p>
 * Типичное использование:<br>
 * Создание экземпляра FB_Base (при этом происходит создание источника данных),
 * который умеет создавать соединения к базе FB_Connection и запросы к базе
 * FB_Query. Запросы к базе данных выполняются в помощью объектов FB_Query.
 * Запрос может создаваться как независимый (в этом случае он сам открывает
 * отдельное соединение к базе при выполнении и закрывает его при закрытии
 * запроса) или как выполняющийся в рамках существующего соединения (в этом
 * случае запрос не закрывает соединение при закрытии, а также не может делать
 * commit и rollback - для объединения запросов в одну транзакцию).
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class FB_Database implements Closeable {

    /**
     * Типы подключения к базе ФБ.
     */
    public static enum EngineType {

        /**
         * Используется встроенный движок ФБ (dll/so, только локальный доступ).
         */
        EMBEDDED,
        /**
         * Используется сервер ФБ для доступа к БД.
         */
        TYPE4
    };
    /**
     * Тип подключения.
     */
    private final EngineType engineType;
    /**
     * Местоположение базы.
     */
    private final String url;
    /**
     * Логин для подключения к базе.
     */
    private final String user;
    /**
     * Пароль для подключения к базе.
     */
    private final String password;
    /**
     * Источник данных (БД).
     */
    private FBSimpleDataSource dataSource;
    /**
     * Сохранение данных в кеше после коммита (также подразумевает полный фетч
     * данных в кеш при выполнении запросов!). Значение по умолчанию для
     * создаваемых соединений.
     */
    private final boolean isResultHoldable;
    /**
     * Автокоммит. Значение по умолчанию для создаваемых соединений.
     */
    private final boolean isAutoCommit;

    /**
     * Конструктор. Инициализация переменных, проверка наличия драйверов.
     *
     * @param isEmbedded Использовать встроенный движок?
     * @param url        Местоположение базы (jdbc:firebirdsql:хост[/порт]:путь
     *                   к бд).
     * @param user       Логин для подключения к базе.
     * @param password   Пароль для подключения к базе.
     * @param charSet    Кодировка подключения.
     * @param isHold     Флаг хранения данных в кеше.
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     */
    public FB_Database(boolean isEmbedded, String url, String user, String password, String charSet, boolean isHold) throws ClassNotFoundException, SQLException {
        this.dataSource = null;
        this.url = url;
        this.engineType = isEmbedded ? EngineType.EMBEDDED : EngineType.TYPE4;
        this.user = user;
        this.password = password;
        this.isAutoCommit = false;
        this.isResultHoldable = isHold;
        // Проверка на существование класса для работы с БД Firebird
        Class.forName("org.firebirdsql.jdbc.FBDriver");
        dataSource = new FBSimpleDataSource();
        dataSource.setDatabase(url);
        dataSource.setType(engineType.toString());
        //DataSource.setEncoding("UTF-8"); // ? win1251, cp1251 - ??????
        dataSource.setCharSet(charSet);
        dataSource.setLoginTimeout(10);
        // Эта опция нужна для вложенных отчетов
        // Если её не включить - возникают ошибки из-за преждевременного
        // закрытия соединения вложенными отчётами
        // Для разработки отчетов в NetBeans в соединение с БД этот параметр
        // тоже должен быть добавлен, пример:
        // jdbc:firebirdsql:<файл бд>?defaultResultSetHoldable=True
        dataSource.setDefaultResultSetHoldable(isResultHoldable);
    }

    /**
     * Конструктор. Упрощенный. См. общий случай.
     *
     * @param isEmbedded Использовать встроенный движок?
     * @param url        Местоположение базы (jdbc:firebirdsql:хост[/порт]:путь
     *                   к бд).
     * @param user       Логин для подключения к базе.
     * @param password   Пароль для подключения к базе.
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     */
    public FB_Database(boolean isEmbedded, String url, String user, String password) throws ClassNotFoundException, SQLException {
        this(isEmbedded, url, user, password, "UTF8", true);
    }

    /**
     * Закрывает пул соединений (чтобы процесс не висел - в вебапп приложение
     * может не завершится).
     */
    @Override
    public void close() {
        if (dataSource != null) {
            // Необходимо было при использовании пула соединений для его освобождения!
            // При неиспользовании пула - соединения отпускаются сразу при закрытии соединения.
            //dataSource.shutdown(); // Принудительное завершение пула соединений.
            dataSource = null;
        }
    }

    public boolean isAutoCommit() {
        return isAutoCommit;
    }

    public boolean isResultHoldable() {
        return isResultHoldable;
    }

    /**
     * Только для FB_Connection.
     *
     * @return
     */
    DataSource getDatasource() {
        return dataSource;
    }

    /**
     * Возвращает URL базы (хост,путь,порт,файл базы данных).
     *
     * @return
     */
    public String getURL() {
        return url;
    }

    /**
     * Проверка на работоспособность.
     *
     * @return true - если инициализация в конструкторе прошла успешно), false -
     *         если не прошла инициализация.
     */
    public boolean isWorked() {
        return (dataSource != null);
    }

    /**
     * Получение соединения к базе.
     *
     * @param user     Логин для подключения к базе.
     * @param password Пароль для подключения к базе.
     * @return Соединение с БД.
     * @throws java.sql.SQLException
     */
    public FB_Connection connect(String user, String password) throws SQLException {
        return new FB_Connection(this, user, password);
    }

    public FB_Connection connect() throws SQLException {
        return new FB_Connection(this, user, password);
    }

    public FB_Connection connect(FB_Connection con) throws SQLException {
        return con != null ? con : connect();
    }

    // Создание запросов.
    public FB_Query query() throws SQLException {
        return new FB_Query(this, null);
    }

    public FB_Query query(String sql) throws SQLException {
        return new FB_Query(this, sql);
    }

    public FB_Query query(FB_Connection con, String sql) throws SQLException {
        return new FB_Query(this, con, sql);
    }

    // Создание и исполнение запросов.
    public FB_Query execute(FB_Connection con, String sql, Object... parameters) throws SQLException {
        return new FB_Query(this, con, sql).execute(parameters);
    }

    public FB_Query execute(String sql, Object... parameters) throws SQLException {
        return new FB_Query(this, sql).execute(parameters);
    }
}
