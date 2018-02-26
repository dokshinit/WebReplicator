package app.model;

import java.time.LocalDateTime;

import static util.DateTools.toMillis;

public class ServiceModel {
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

    public String errMessage;

    public int delayTime;

    public ServiceModel(int delay) {
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

        errMessage = null;

        delayTime = delay;
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

        dst.errMessage = errMessage;
        dst.delayTime = delayTime;
    }

    public synchronized void startReplicate() {
        curStartTime = LocalDateTime.now();
        curEndTime = null;
        curRowCount = 0;
        isReplication = true;
        for (TabInfo t : tabs) t.clear();
        curTab = -1;
        xver = xver_max = 0;
        errMessage = null;
    }

    public synchronized void endReplicate() {
        curEndTime = LocalDateTime.now();
        if (errMessage == null) {
            allCount++;
            allMsec += toMillis(curEndTime) - toMillis(curStartTime);
            allRowCount += curRowCount;
        }
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

    public synchronized void setError(String err) {
        errMessage = err;
    }
}
