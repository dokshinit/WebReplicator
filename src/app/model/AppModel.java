package app.model;

import app.App;
import app.ExError;
import fbdbengine.FB_Connection;
import fbdbengine.FB_CustomException;
import fbdbengine.FB_Database;
import fbdbengine.FB_Query;

import java.sql.SQLException;
import java.time.LocalDateTime;

import static app.App.logger;
import static app.App.updateState;
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

    public void loadConfig() {
    }

    public void configureDB() throws ExError {
        try {
            dbCenter = new FB_Database(false, "127.0.0.1:Center", "SYSDBA", "xxxxxxxx", "UTF-8", false);
            dbWeb = new FB_Database(false, "127.0.0.1:WebCenter", "SYSDBA", "xxxxxxxx", "UTF-8", false);
        } catch (Exception ex) {
            throw new ExError("Ошибка настройки параметров БД!", ex);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Вспомогательный инструментарий для операций с БД.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /** Интерфейс для вызова обработчика операции с БД. */
    @FunctionalInterface
    interface QFBTask {
        void run(final FB_Connection con) throws ExError, SQLException, Exception;
    }

    /** Хелпер для операций с БД. */
    void QFB(FB_Database db, QFBTask task) throws ExError {
        FB_Connection con;
        try {
            con = db.connect(); // Соединение

        } catch (Exception ex) {
            FB_CustomException e = FB_CustomException.parse(ex);
            if (e != null) throw new ExError(ex, "Ошибка подключения к БД: %s", e.name + ": " + e.message);
            logger.error("Ошибка подключения к БД!", ex);
            throw new ExError(ex, "Ошибка подключения к БД! Детальная информация в логе.");
        }
        // Соединение установлено.
        try {
            task.run(con);

        } catch (ExError ex) {
            logger.error(ex.getMessage());
            throw ex;

        } catch (Exception ex) {
            FB_CustomException e = FB_CustomException.parse(ex);
            if (e != null) throw new ExError(ex, "Ошибка операции БД: %s", e.name + ": " + e.message);
            logger.error("Ошибка операции БД!", ex);
            throw new ExError(ex, "Ошибка операции БД! Детальная информация в логе.");

        } finally {
            // Если не внешнее - закрываем с роллбэк (если нужно сохранение данных - это надо сделать в теле задачи).
            FB_Connection.closeSafe(con);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Репликация БД.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static class TabInfo {
        public String name;
        public String title;
        public LocalDateTime startTime, endTime;
        public int count;
        public int index;
        public long xver_max;

        public TabInfo(String name, String title) {
            this.name = name;
            this.title = title;
            clear();
        }

        public synchronized void clear() {
            startTime = null;
            endTime = null;
            count = 0;
            index = 0;
            xver_max = 0;
        }

        public synchronized void copyTo(TabInfo dst) {
            dst.name = name;
            dst.title = title;
            dst.startTime = startTime;
            dst.endTime = endTime;
            dst.count = count;
            dst.index = index;
            dst.xver_max = xver_max;
        }

        public synchronized void start() {
            startTime = LocalDateTime.now();
            xver_max = 0;
        }

        public synchronized void end(int index, long xver_max) {
            this.index = index;
            this.count = index;
            this.xver_max = xver_max;
            this.endTime = LocalDateTime.now();
        }

        public synchronized void initCount(int count) {
            this.index = 0;
            this.count = count;
        }

        public synchronized void updateIndex(int index, long xver_max) {
            this.index = index;
            this.xver_max = xver_max;
        }
    }

    public static class ServiceModel {
        public LocalDateTime startTime;

        public int allCount;
        public long allMsec;
        public int allRowCount;

        public LocalDateTime curStartTime, curEndTime;
        public long curRowCount; // кол-во реплицированных строк.

        public TabInfo[] tabs;
        public int curTab;
        public boolean isReplication;

        public long xver, xver_max;

        public ServiceModel() {
            startTime = LocalDateTime.now();
            allCount = 0;
            allMsec = 0;
            allRowCount = 0;

            curStartTime = null;
            curEndTime = null;
            curRowCount = 0; // кол-во реплицированных строк.

            tabs = new TabInfo[]{
                    new TabInfo("AZS", "Лицевые счета"),
                    new TabInfo("CARD", "Лицевые счета"),
                    new TabInfo("CLIENT", "Лицевые счета"),
                    new TabInfo("CONTRACT", "Лицевые счета"),
                    new TabInfo("REGISTRY", "Лицевые счета"),
                    new TabInfo("ACC", "Лицевые счета"),
                    new TabInfo("TRANS", "Лицевые счета"),
                    new TabInfo("PAY", "Лицевые счета")
            };
            curTab = -1;
            isReplication = false;

            xver = 0;
            xver_max = 0;
        }

        public synchronized void copyTo(ServiceModel dst) {
            dst.startTime = startTime;
            dst.allCount = allCount;
            dst.allMsec = allMsec;
            dst.allRowCount = allRowCount;
            dst.curStartTime = curStartTime;
            dst.curEndTime = curEndTime;
            dst.curRowCount = curRowCount;
            for (int i = 0; i < tabs.length; i++) tabs[i].copyTo(dst.tabs[i]);
            dst.curTab = curTab;
            dst.isReplication = isReplication;
            dst.xver = xver;
            dst.xver_max = xver_max;
        }

        public synchronized void startReplicate() {
            curStartTime = LocalDateTime.now();
            curEndTime = null;
            curRowCount = 0;
            isReplication = true;
            for (TabInfo t : tabs) t.clear();
            curTab = -1;
            xver = xver_max = 0;
        }

        public synchronized void endReplicate() {
            curEndTime = LocalDateTime.now();
            allCount++;
            allMsec += toMillis(curEndTime) - toMillis(curStartTime);
            allRowCount += curRowCount;
            curTab = -1;
            isReplication = false;
        }

        public synchronized void setXVer(long xver) {
            this.xver = xver;
        }

        public synchronized void startReplicateTable(int i) {
            curTab = i;
        }

        public synchronized void endReplicateTable() {
            curRowCount += tabs[curTab].index;
            xver_max = Math.max(xver_max, tabs[curTab].xver_max);
            if (curTab == tabs.length-1) curTab = -1; // После последней таблицы.
        }
    }


    private FB_Connection src, dst;
    public ServiceModel curModel;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public void replicate() throws ExError {
        curModel.startReplicate();
        logger.info("Старт репликации...");

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
                curModel.setXVer(qdst.getLong("X_VER"));
                //logger.infof("X_VER = %d", xver);
                qdst.closeSafe();

                //xver = 0L;

                ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                // Репликация таблиц.
                for (int i = 0; i < curModel.tabs.length; i++) {
                    curModel.startReplicateTable(i);
                    replicateTable(curModel.tabs[i]);
                    curModel.endReplicateTable();
                }

                ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                // Запись новой версии (если увеличилась).
                if (curModel.xver_max > curModel.xver) {
                    qdst = dst.execute("EXECUTE PROCEDURE WR_XVER_SET(?)", curModel.xver_max);
                    //logger.infof("X_VERNEW = %d", xver_max);
                    qdst.closeSafe();
                }

                dst.commit();

            });
        });
        curModel.endReplicate();
        logger.infof("Завершение репликации: время=%s, строк=%d", formatHHMMSS(toMillis(curModel.curEndTime)-toMillis(curModel.curEndTime)), curModel.curRowCount);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Реализация репликации Таблицы.
    private static final String params_ = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,";

    private String procParams(int n) {
        if (n > 0) return "(" + params_.substring(0, n * 2 - 1) + ")";
        return "";
    }

    private void replicateTable(TabInfo tab) throws Exception {
        tab.start();
        //logger.infof("%s: CALC COUNT FOR IMPORT...", tab.name);

        FB_Query qsrc = src.execute("SELECT count(*) FROM WR_EXPORT_" + tab.name + "(?)", curModel.xver);
        qsrc.next();
        tab.initCount(qsrc.getInteger(1));
        qsrc.closeSafe();
        //logger.infof("%s: FOR IMPORT = %d", tab.name, tab.count);

        qsrc = src.execute("SELECT * FROM WR_EXPORT_" + tab.name + "(?)", curModel.xver);
        int n = qsrc.rs().getMetaData().getColumnCount();
        FB_Query qdst = dst.query("EXECUTE PROCEDURE WR_IMPORT_" + tab.name + procParams(n));

        int index = 0;
        long xver_max = tab.xver_max;
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

        tab.end(index, xver_max);
        //logger.infof("TIME = %s", formatHHMMSS(tab.timeMsec));
    }

}
