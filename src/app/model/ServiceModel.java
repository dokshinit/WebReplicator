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
                new TabInfo("AZS", "АЗС"),
                new TabInfo("CARD", "Карты ТК"),
                new TabInfo("CLIENT", "Клиенты ТК"),
                new TabInfo("CONTRACT", "Контракты ТК"),
                new TabInfo("REGISTRY", "Реестр настроек"),
                new TabInfo("ACC", "Лицевые счета ТК"),
                new TabInfo("TRANS", "Транзакции ТК"),
                new TabInfo("PAY", "Оплаты ТК")
        };
        curTab = -1;
        isReplication = false;

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

    public synchronized void startReplicateTable(int i) {
        curTab = i;
    }

    public synchronized void endReplicateTable() {
        curRowCount += tabs[curTab].index;
        if (curTab == tabs.length-1) curTab = -1; // После последней таблицы.
    }

    public synchronized void setError(String err) {
        errMessage = err;
    }
}
