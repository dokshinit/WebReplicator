package app.model;

import app.ExError;
import fbdbengine.FB_Connection;
import fbdbengine.FB_CustomException;
import fbdbengine.FB_Database;
import fbdbengine.FB_Query;
import org.firebirdsql.jdbc.FBSQLException;
import xconfig.XConfig;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

import static app.App.isUI;
import static app.App.logger;

/**
 * Модель приложения (сессии пользователя)
 *
 * @author Aleksey Dokshin <dant.it@gmail.com> (28.11.17).
 */
public class AppModel {

    private FB_Database dbCenter, dbWeb;

    public AppModel() {
    }

    public int redrawInterval;
    public String statePath;
    public ServiceModel replModel;

    public void init() throws ExError {
        int delay;
        String src_base, src_user, src_password;
        String dst_base, dst_user, dst_password;

        logger.infof("Загрузка конфигурации...");
        try {
            XConfig cfg = new XConfig();
            cfg.load("app.config");

            src_base = cfg.getKey("db-src.host", "192.168.1.6") + ":" + cfg.getKey("db-src.alias", "Center");
            src_user = cfg.getKey("db-src.user", "LKREPLICATOR");
            src_password = cfg.getKey("db-src.password", "xxxxxxxx");

            dst_base = cfg.getKey("db-dst.host", "127.0.0.1") + ":" + cfg.getKey("db-dst.alias", "WebCenter");
            dst_user = cfg.getKey("db-dst.user", "REPLICATOR");
            dst_password = cfg.getKey("db-dst.password", "xxxxxxxx");

            delay = cfg.getIntKey("replicator.delay", 30000);

            if (isUI) {
                redrawInterval = cfg.getIntKey("ui.redraw", 250);
            } else {
                redrawInterval = cfg.getIntKey("noui.redraw", 5000);
                statePath = cfg.getKey("noui.path", "./state");
            }

        } catch (Exception ex) {
            src_base = "192.168.1.6:Center";
            src_user = "LKREPLICATOR";
            src_password = "xxxxxxxx";
            dst_base = "127.0.0.1:WebCenter";
            dst_user = "REPLICATOR";
            dst_password = "xxxxxxxx";
            delay = 30000;
            redrawInterval = isUI ? 250 : 5000;
            statePath = "./state";

            logger.infof("Ошибка загрузки конфигурации: %s! Приняты параметры по умолчанию!", ex.getMessage());
        }

        logger.infof("Настройка подключения к БД...");
        try {
            dbCenter = new FB_Database(false, src_base, src_user, src_password, "UTF-8", false);
            dbWeb = new FB_Database(false, dst_base, dst_user, dst_password, "UTF-8", false);
        } catch (Exception ex) {
            throw new ExError("Ошибка настройки параметров БД!", ex);
        }

        replModel = new ServiceModel(delay);
    }


    /** Интерфейс для вызова обработчика операции с БД. */
    @FunctionalInterface
    interface QFBTask {
        @SuppressWarnings("DuplicateThrows")
        void run(final FB_Connection con) throws ExError, SQLException, Exception;
    }

    /** Хелпер для операций с БД. */
    void QFB(FB_Database db, QFBTask task) throws ExError {
        String dbname = db == dbCenter ? "Center" : "Web";
        FB_Connection con = null;
        try {
            try {
                con = db.connect(); // Соединение.
                con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ); // Полный снапшот.
                con.setTransactionWait(false); // Исключения по блокировки без ожидания коммита.

            } catch (Exception ex) {
                FB_CustomException e = FB_CustomException.parse(ex);
                if (e != null)
                    throw new ExError(ex, "Ошибка подключения к БД(%s): %s", dbname, e.name + ": " + e.message);
                logger.errorf(ex, "Ошибка подключения к БД(%s)!", dbname);
                throw new ExError(ex, "Ошибка подключения к БД(%s)! Детальная информация в логе.", dbname);
            }
            // Соединение установлено.
            try {
                task.run(con);

            } catch (ExError ex) {
                logger.error(ex.getMessage());
                throw ex;

            } catch (Exception ex) {
                FB_CustomException e = FB_CustomException.parse(ex);
                if (e != null) throw new ExError(ex, "Ошибка операции БД(%s): %s", dbname, e.name + ": " + e.message);
                logger.errorf(ex, "Ошибка операции БД(%s)!", dbname);
                throw new ExError(ex, "Ошибка операции БД(%s)! Детальная информация в логе.", dbname);

            }

        } finally {
            // Если не внешнее - закрываем с роллбэк (если нужно сохранение данных - это надо сделать в теле задачи).
            FB_Connection.closeSafe(con);
        }
    }

    /** Репликация всех данных. */
    public void replicate() throws ExError {
        replModel.startReplicate();
        //logger.info("Старт репликации...");

        try {
            try {
                // Репликация идёт в одном соединении и одной транзакции.
                QFB(dbCenter, (conSrc) -> {
                    QFB(dbWeb, (conDst) -> {

                        // Репликация таблиц.
                        for (int i = 0; i < replModel.tabs.length; i++) {
                            TabInfo tab = replModel.tabs[i];
                            replModel.startReplicateTable(i);
                            replicateTable(conSrc, conDst, tab);
                            replModel.endReplicateTable();
                            if (tab.isError()) throw new ExError("Ошибка[%s] %s!", tab.name, tab.msgError());
                        }

                        conDst.commit();
                        conSrc.commit();

                    });
                });

            } catch (Exception ex) {
                replModel.setError(ex.getMessage());
                throw ex;
            }
        } finally {
            replModel.endReplicate();
        }
    }

    /** Реализация репликации одной таблицы. */
    private void replicateTable(FB_Connection conSrc, FB_Connection conDst, TabInfo tab) {
        tab.start();
        //logger.infof("%s: CALC COUNT FOR IMPORT...", tab.name);

        FB_Query qSrc = null, qDst = null;
        int index = 0, upd = 0, err = 0;
        try {
            qSrc = conSrc.execute("EXECUTE PROCEDURE WR_EXPORT_" + tab.name + "_COUNT");
            qSrc.next();
            tab.initCount(qSrc.getInteger(1));
            qSrc.close();
            //logger.infof("%s: FOR IMPORT = %d", tab.name, tab.count);

            if (tab.count > 0) { // Если записей для репликации нет, то и не запускаем саму репликацию (чтобы удаление не дергать)!
                qSrc = conSrc.execute("SELECT * FROM WR_EXPORT_" + tab.name);
                int n = qSrc.getMetaData().getColumnCount();
                qDst = conDst.query("EXECUTE PROCEDURE WR_IMPORT_" + tab.name + " (" + FB_Query.buildProcParamsSQL(n) + ")");

                Object[] vals = new Object[n];
                while (qSrc.get(vals)) {
                    qDst.execute(vals);
                    if (qDst.next()) {
                        if (qDst.getInteger(1) == 0) upd += 1;
                    } else {
                        err++;
                    }
                    index++;
                    if (index % 10000 == 0) {
                        tab.updateIndex(index);
                        //logger.infof("%s: IMPORTED = %d (%.1f%%)", tab.name, tab.count, tab.count == 0 ? 0 : tab.index * 100.0f / tab.count);
                    }
                }
                //logger.infof("%s: IMPORTED = %d (100%%)", tab.name, tab.index);
                qDst.close();
                qSrc.close();
            }

            tab.end(index);

        } catch (Exception e) {
            FB_Query.closeSafe(qDst);
            FB_Query.closeSafe(qSrc);
            if (e instanceof FBSQLException) {
                FBSQLException ex = (FBSQLException) e;
                if (ex.getErrorCode() == 335544345) {
                    tab.end(index, "Таблица заблокирована!");
                } else {
                    FB_CustomException exx = FB_CustomException.parse(e);
                    if (exx != null) {
                        tab.end(index, "FBEx[" + exx.id + ":" + exx.name + "] " + exx.message);
                    } else {
                        tab.end(index, "FB: Ошибка репликации " + tab.name + "!");
                    }
                }
            } else {
                //tab.end(index, ExError.exMsg(e));
                tab.end(index, "EX: Ошибка репликации " + tab.name + "!");
            }
            logger.error(tab.msgError(), e);
        }
        //logger.infof("TIME = %s", formatHHMMSS(ChronoUnit.MILLIS.between(tab.startTime, tab.endTime)));
    }

    /** Создание каталога, если не существует. */
    @SuppressWarnings("Duplicates")
    public static File createDirectoryIfNotExist(String path) throws ExError {
        try {
            File dir = new File(path);
            if (!dir.exists()) {
                if (!dir.mkdir()) {
                    throw new ExError("Ошибка создания каталога!");
                }
            }
            return dir;
        } catch (ExError ex) {
            throw ex;
        } catch (Exception e) {
            throw new ExError(e, "Ошибка создания каталога!");
        }
    }

    /** Безопасное удаление файла. */
    public static void deleteFileSafe(String path) {
        try {
            File file = new File(path);
            if (file.exists()) {
                // Если не удалился сразу - пробуем удалить при выходе на случай, если залочен.
                if (!file.delete()) file.deleteOnExit();
            }
        } catch (Exception ignore) {
        }
    }
}
