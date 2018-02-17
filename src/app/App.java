/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package app;

import app.model.*;
import util.CommonTools;
import util.StringTools;

import java.time.LocalDateTime;
import java.util.logging.Level;

import static app.model.Helper.fmtDT84;
import static app.model.Helper.fmtDT86;
import static util.DateTools.formatHHMMSS;
import static util.DateTools.toMillis;

/**
 * Приложение для формирования и ведения "Кассовой книги" АЗС.
 * <p>
 * <ol> <li> АРМ при закрытии смены выгружает информацию о смене для внешних приложений. Данные формируются в виде
 * текстовых файлов формата xconfig с генерацией имён в заданном формате (session_{azs:3}_{dtw:422}_{idd:5}.export) в
 * каталог Data\SessionExport. <li> Приложение запускается после закрытия каждой смены (после печати отчётов). <li>
 * Загружается база данных приложения. <li> Сканируется каталог и найденные экспортные файлы заносятся в базу данных
 * приложения. При этом успешно импортированные удаляются из каталога. <li> Производится проверка всех данных в базе на
 * корректность. Некорректные записи подсвечиваются и содержат коментарий к ошибке. <li> Оператор может: <ul> <li>
 * Корректировать данные в записях смен; <li> Производить автоматический перерасчёт переходящего сальдо смен; <li>
 * Формировать печатные формы кассовой книги и выводить их на печать; </ul> <li> Оператор даёт команду на завершение
 * приложения (переход к открытию смены в АРМ). </ol>
 * <p>
 * Приложение хранит данные в конфигурационных файлах XCconfig. <ol> <li>App.config - конфигурация приложения;
 * <li>data\sessions.config - данные о сменах; <li>data\books.config - данные кассовой книги. </ol>
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class App {

    /**
     * Логгер для вывода отладочной информации.
     */
    public final static LoggerExt logger = LoggerExt.getNewLogger("WebReplicator");
    /**
     * Модель приложения.
     */
    public final static AppModel model = new AppModel();

    public static boolean isWindowsOS = false;
    public static boolean isLinuxOS = false;

    public static String homeDir = "";

    /**
     * Точка старта приложения.
     *
     * @param args Параметры.
     */
    public static void main(String[] args) {

        try {
            initLog(); // Инициализация логгера.

            initDB(); // Настройка параметров БД.

            startApp(); // Старт приложения.

        } catch (Exception ex) {
            logger.error("Ошибка запуска приложения!", ex);
        }
    }

    /**
     * Инициализация лога.
     *
     * @throws java.io.IOException
     */
    private static void initLog() {
        String s = System.getProperty("os.name", "").toLowerCase().substring(0, 3);
        if (s.equals("win")) isWindowsOS = true;
        if (s.equals("lin")) isLinuxOS = true;

        // Отключаем вывод лога в консоль.
        LoggerExt.removeConsoleOutput();
        // Логгируем все сообщения.
        logger.setLevel(Level.ALL);
        // Настраиваем логгер для файлового вывода.
        logger.enable(true).toFile();

        logger.config("Инициализация приложения");
    }

    private static void initDB() throws Exception {
        logger.config("Настройка параметров БД");
        try {
            model.configureDB();

        } catch (Exception ex) {
            logger.error("Ошибка настройки параметров БД!", ex);
            throw ex;
        }
    }

    private static boolean isTerminated = false;

    private static boolean safeTermSleep(int time) {
        if (!CommonTools.safeInterruptedSleep(time)) {
            isTerminated = true;
            return true;
        }
        return false;
    }

    private static int stateUpdateInterval = 200; // миллисекунды

    private static void startApp() throws Exception {

//        ANSIOut out = new ANSIOut();
//        int dx = out.getWidth();
//        int dy = out.getHeight();
//
//        out.attr(0, 40, 37).clear();
//
//        int sx = 1, sy = 1;
//        for (int y = 0; y < 16; y++) {
//            for (int x = 0; x < 16; x++) {
//                //out.at(sx + x * 2, sy + y).bgcolor(x + y * 16).print("AA");
//                int c = x + y * 16;
//                out.at(sx + x * 5, sy + y).bgcolor(c).print(" %03d ", c);
//            }
//        }
//        out.attr(0);
//        if (true) return;

        model.curModel = new AppModel.ServiceModel();

        out.cursorOff().attr(0).clear().at(1, 1);

        new Thread(() -> {
            // Крутим цикл репликации.
            while (!isTerminated) {
                try {
                    model.replicate();
                } catch (Exception ex) {
                    logger.errorf(ex, "Ошибка репликации!");
                    //break;
                }
                // Пауза между репликациями.
                if (safeTermSleep(5000)) break;
            }
            isTerminated = true;
        }).start();

        while (!isTerminated) {
            updateState();
            safeTermSleep(stateUpdateInterval);
        }

        out.cursorOn();
    }

    private static ANSIOut out = new ANSIOut();

    private static LocalDateTime stateUpdateTime = null;
    private static int w = 80;
    private static final String delimS = StringTools.fill('─', w);
    private static final String delimSB = StringTools.fill('━', w);
    private static final String delimD = StringTools.fill('═', w);
    private static final String delim1_4 = StringTools.fill('▂', w);
    private static final String delim1_2 = StringTools.fill('▄', w);
    private static final String delim3_4 = StringTools.fill('▆', w);

    private static final String delim111 = StringTools.fill('─', w - 20);

    private static AppModel.ServiceModel CM = new AppModel.ServiceModel();

    private static int bgtitle = 19, bgbase = 17;

    public static void updateState() {
        stateUpdateTime = LocalDateTime.now();
        model.curModel.copyTo(CM);
        //
        String h1 = "---", h2 = "---", h3 = "---";
        String s1 = "---", s2 = "---";
        long time = System.currentTimeMillis();
        if (CM.startTime != null) {
            h1 = fmtDT86(CM.startTime);
            h2 = formatHHMMSS(System.currentTimeMillis() - toMillis(CM.startTime));
            h3 = String.format("%d, время %s", CM.allCount, formatHHMMSS(CM.allMsec));

            if (CM.isReplication) {
                s1 = String.format("%s", fmtDT86(CM.curStartTime));
                s2 = String.format("%s", formatHHMMSS(time - toMillis(CM.curStartTime)));
            } else {
                if (CM.curStartTime != null) {
                    s1 = String.format("%s, Завершение: %s", fmtDT86(CM.curStartTime), fmtDT86(CM.curEndTime));
                    s2 = String.format("%s", formatHHMMSS(toMillis(CM.curStartTime) - toMillis(CM.curStartTime)));
                }
            }
        }
        //
        out.reset().at(1, 1);
        out.color(15, bgtitle).bold().print(w, " «Сервис репликации БД»").boldOff()
                .atX(w - 19).bold().color(230).println(fmtDT86(stateUpdateTime)).boldOff();
        out.color(45).print(w, " v2018.02.15").atX(w - 46).color(123).print("© Докшин Алексей Николаевич, ")
                .color(49).underline().println("dokshin@gmail.com").underlineOff();
        out.color(bgbase, 18).println(delim3_4).color(7, bgbase);

        int c1 = 18, c2 = 19;
        //
        out.println(w, " Время начала работы : %s", h1);
        out.println(w, " Общее время работы  : %s", h2);
        out.println(w, " Репликаций          : %s", h3);

        out.color(c1).println(delimSB).color(7, bgbase);
        out.print(w, CM.isReplication ? " Текущая репликация" : " Последняя репликация");
        out.println(w, "   Начало: %s", s1);
        out.println(w, "   Длительность: %s", s2);

        out.color(c1).println(delimS).color(7, bgbase);
        for (int i = 0; i < CM.tabs.length; i++) {
            AppModel.TabInfo tab = CM.tabs[i];
            if (CM.curTab == i) {
                long percent = tab.count == 0 ? 0 : tab.index * 100L / tab.count;
                out.bold().color(11, 20).print(w, " ▶ ");
                out.color(15).print("Таблица: %s (обработка)", tab.name);
                out.atX(w - 7 - 6 - 19).print("%17s", String.format("[%d/%d]", tab.index, tab.count));
                out.atX(w - 7 - 6).print("%3d%%", percent);
                out.atX(w - 7).print("%s", formatHHMMSS(time - toMillis(tab.startTime)));
                out.boldOff();
            } else {
                if (tab.startTime != null) { // ○◉□▣
                    long percent = tab.count == 0 ? 100 : tab.index * 100L / tab.count;
                    out.color(10, bgbase).print(w, " ▣ ").color(7).print("Таблица: %s", tab.name);
                    out.atX(w - 7 - 6 - 19).print("%17s", String.format("[%d/%d]", tab.index, tab.count));
                    out.atX(w - 7 - 6).print("%3d%%", percent);
                    out.atX(w - 7).print("%s", formatHHMMSS(toMillis(tab.endTime) - toMillis(tab.startTime)));
                } else {
                    out.color(21, bgbase).print(w, " □ ").color(7).print("Таблица: %s", tab.name);
                }
            }
            out.println();
        }

        //
        out.color(18, bgbase).println(delim1_4);
    }
}