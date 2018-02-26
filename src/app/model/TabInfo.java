package app.model;

import java.time.LocalDateTime;

public class TabInfo {
    public String name;
    public String title;
    public LocalDateTime startTime, endTime;
    public int count;
    public int index;
    public long xver_max;
    public boolean isError;

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
        isError = false;
    }

    public synchronized void copyTo(TabInfo dst) {
        dst.name = name;
        dst.title = title;
        dst.startTime = startTime;
        dst.endTime = endTime;
        dst.count = count;
        dst.index = index;
        dst.xver_max = xver_max;
        dst.isError = isError;
    }

    public synchronized void start() {
        startTime = LocalDateTime.now();
        xver_max = 0;
        isError = false;
    }

    public synchronized void end(int index, long xver_max, boolean iserror) {
        this.index = index;
        if (!iserror) {
            this.count = index;
            this.xver_max = xver_max;
        }
        this.endTime = LocalDateTime.now();
        this.isError = iserror;
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
