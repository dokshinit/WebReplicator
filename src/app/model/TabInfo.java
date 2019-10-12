package app.model;

import java.time.LocalDateTime;

public class TabInfo {
    public String name;
    public String title;
    public LocalDateTime startTime, endTime;
    public int count;
    public int index;
    /** Если не null, то содержит текст ошибки репликации. */
    private String msgError;

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
        msgError = null;
    }

    public synchronized void copyTo(TabInfo dst) {
        dst.name = name;
        dst.title = title;
        dst.startTime = startTime;
        dst.endTime = endTime;
        dst.count = count;
        dst.index = index;
        dst.msgError = msgError;
    }

    public synchronized void start() {
        startTime = LocalDateTime.now();
        msgError = null;
    }

    public synchronized void end(int index) {
        this.index = index;
        this.count = index;
        this.endTime = LocalDateTime.now();
    }

    public synchronized void end(int index, String msgError) {
        this.index = index;
        this.endTime = LocalDateTime.now();
        this.msgError = msgError;
    }

    public synchronized boolean isError() {
        return msgError != null;
    }

    public synchronized String msgError() {
        return msgError == null ? "" : msgError;
    }

    public synchronized void initCount(int count) {
        this.index = 0;
        this.count = count;
    }

    public synchronized void updateIndex(int index) {
        this.index = index;
    }
}
