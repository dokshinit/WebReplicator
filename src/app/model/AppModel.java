package app.model;

import app.ExError;
import fbdbengine.FB_Connection;
import fbdbengine.FB_CustomException;
import fbdbengine.FB_Database;
import fbdbengine.FB_Query;
import xconfig.XConfig;

import java.io.File;
import java.sql.SQLException;

import static app.App.isUI;
import static app.App.logger;
import static util.DateTools.*;

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


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Вспомогательный инструментарий для операций с БД.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Интерфейс для вызова обработчика операции с БД.
     */
    @FunctionalInterface
    interface QFBTask {
        void run(final FB_Connection con) throws ExError, SQLException, Exception;
    }

    /**
     * Хелпер для операций с БД.
     */
    void QFB(FB_Database db, QFBTask task) throws ExError {
        String dbname = db == dbCenter ? "Center" : "Web";
        FB_Connection con;
        try {
            con = db.connect(); // Соединение

        } catch (Exception ex) {
            FB_CustomException e = FB_CustomException.parse(ex);
            if (e != null) throw new ExError(ex, "Ошибка подключения к БД(%s): %s", dbname, e.name + ": " + e.message);
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

        } finally {
            // Если не внешнее - закрываем с роллбэк (если нужно сохранение данных - это надо сделать в теле задачи).
            FB_Connection.closeSafe(con);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Репликация БД.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private FB_Connection src, dst;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public void replicate() throws ExError {
        replModel.startReplicate();
        logger.info("Старт репликации...");

        try {
            try {
                // Репликация идёт в одном соединении и одной транзакции.
                QFB(dbCenter, (srcCon) -> {
                    QFB(dbWeb, (dstCon) -> {

                        src = srcCon;
                        dst = dstCon;

                        FB_Query qdst;

                        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        // Запрос текущей версии.
                        qdst = dst.execute("EXECUTE PROCEDURE WR_XVER_GET");
                        if (!qdst.next()) throw new ExError("Ошибка чтения X_VER!");
                        replModel.setXVer(qdst.getLong("X_VER"));
                        //logger.infof("X_VER = %d", xver);
                        qdst.closeSafe();

                        //xver = 0L;

                        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        // Репликация таблиц.
                        for (int i = 0; i < replModel.tabs.length; i++) {
                            TabInfo tab = replModel.tabs[i];
                            replModel.startReplicateTable(i);
                            replicateTable(tab);
                            replModel.endReplicateTable();
                            if (tab.isError) throw new ExError("Ошибка репликации таблицы %s!", tab.name);
                        }

                        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        // Запись новой версии (если увеличилась).
                        if (replModel.xver_max > replModel.xver) {
                            qdst = dst.execute("EXECUTE PROCEDURE WR_XVER_SET(?)", replModel.xver_max);
                            //logger.infof("X_VERNEW = %d", xver_max);
                            qdst.closeSafe();
                        }

                        dst.commit();

                    });
                });

            } catch (Exception ex) {
                replModel.setError(ex.getMessage());
                throw ex;
            }
        } finally {
            replModel.endReplicate();
            logger.infof("Завершение репликации: время=%s, строк=%d", formatHHMMSS(toMillis(replModel.curEndTime) - toMillis(replModel.curEndTime)), replModel.curRowCount);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Реализация репликации Таблицы.
    private static final String params_ = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,";

    private String procParams(int n) {
        if (n > 0) return "(" + params_.substring(0, n * 2 - 1) + ")";
        return "";
    }

    private void replicateTable(TabInfo tab) {
        tab.start();
        //logger.infof("%s: CALC COUNT FOR IMPORT...", tab.name);

        int index = 0;
        long xver_max = tab.xver_max;
        try {
            FB_Query qsrc = src.execute("SELECT count(*) FROM WR_EXPORT_" + tab.name + "(?)", replModel.xver);
            qsrc.next();
            tab.initCount(qsrc.getInteger(1));
            qsrc.closeSafe();
            //logger.infof("%s: FOR IMPORT = %d", tab.name, tab.count);

            qsrc = src.execute("SELECT * FROM WR_EXPORT_" + tab.name + "(?)", replModel.xver);
            int n = qsrc.rs().getMetaData().getColumnCount();
            FB_Query qdst = dst.query("EXECUTE PROCEDURE WR_IMPORT_" + tab.name + procParams(n));

            Object[] vals = new Object[n];
            while (qsrc.get(vals)) {
                qdst.execute(vals);
                index++;
                xver_max = Math.max(xver_max, qsrc.getLong("X_VER"));
                if (index % 100 == 0) {
                    tab.updateIndex(index, xver_max);
                    //    logger.infof("%s: IMPORTED = %d (%.1f%%)", tab.name, tab.count, tab.count == 0 ? 0 : tab.index * 100.0f / tab.count);
                }
            }
            //logger.infof("%s: IMPORTED = %d (100%%)", tab.name, tab.index);
            qsrc.closeSafe();
            qdst.closeSafe();

            tab.end(index, xver_max, false);

        } catch (Exception ex) {
            tab.end(index, xver_max, true);
            logger.error("", ex);
            //logger.infof("TIME = %s", formatHHMMSS(tab.timeMsec));
        }
    }

    /**
     * Создание каталога, если не существует.
     */
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

    /**
     * Безопасное удаление файла.
     */
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
