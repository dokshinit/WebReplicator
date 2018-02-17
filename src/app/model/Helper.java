package app.model;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static util.NumberTools.*;

/**
 * @author Aleksey Dokshin <dant.it@gmail.com> (27.12.17).
 */
public class Helper {

    public static String fmtN2(Long num) {
        return FMT_N2T.format(num / 100.0);
    }

    public static String fmtN2_0(Long num) {
        return FMT_N0T.format(num / 100.0);
    }

    public static String fmtN3_2(Long num) {
        return FMT_N2T.format(Math.round(num / 10.0) / 100.0);
    }

    public static String fmtN3(Long num) {
        return FMT_N3T.format(num / 1000.0);
    }

    public static final DateTimeFormatter FMT_DT_DDMMYYYYHHMMSS = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
    public static final DateTimeFormatter FMT_DT_DDMMYYYYHHMM = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm");
    public static final DateTimeFormatter FMT_DATE_DDMMYYYY = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    public static final DateTimeFormatter FMT_DATE_DDMMYY = DateTimeFormatter.ofPattern("dd.MM.yy");
    public static final LocalDate DATE_2100 = LocalDate.of(2100, 01, 01);

    public static String fmtDate8(LocalDate dt) {
        if (DATE_2100.equals(dt)) return "—";
        return FMT_DATE_DDMMYYYY.format(dt);
    }

    public static String fmtDate6(LocalDate dt) {
        if (DATE_2100.equals(dt)) return "—";
        return FMT_DATE_DDMMYY.format(dt);
    }

    public static String fmtDT86(LocalDateTime dt) {
        if (DATE_2100.equals(dt)) return "—";
        return FMT_DT_DDMMYYYYHHMMSS.format(dt);
    }

    public static String fmtDT84(LocalDateTime dt) {
        if (DATE_2100.equals(dt)) return "—";
        return FMT_DT_DDMMYYYYHHMM.format(dt);
    }
}
