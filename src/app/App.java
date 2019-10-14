/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package app;

import app.model.*;
import util.CommonTools;
import util.StringTools;

import java.io.File;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.logging.Level;

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

    public static boolean isUI = false;

    /**
     * Точка старта приложения.
     *
     * @param args Параметры.
     */
    public static void main(String[] args) {

        Runtime.getRuntime().addShutdownHook(new Thread(App::stopApp, "Shutdown-thread"));

        for (String s : args) {
            if ("showui".equals(s)) isUI = true;
        }

        try {
            initLog(); // Инициализация логгера.

            initModel(); // Настройка модели.

            startApp(); // Старт приложения.

        } catch (Exception ex) {
            logger.error("Ошибка запуска приложения!", ex);
        }
    }

    /**
     * Инициализация лога.
     */
    private static void initLog() {
        // Отключаем вывод лога в консоль.
        LoggerExt.removeConsoleOutput();
        // Логгируем все сообщения.
        logger.setLevel(Level.ALL);
        // Настраиваем логгер для файлового вывода.
        logger.enable(true).toFile();

        logger.configf("Инициализация приложения (%s)", isUI ? "c UI" : "без UI");
    }

    /**
     * Настройка модели.
     */
    private static void initModel() throws Exception {
        logger.config("Создание модели...");
        try {
            model.init();

        } catch (Exception ex) {
            logger.error("Ошибка создания модели!", ex);
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

    private static void startApp() throws Exception {

        if (!isUI) {
            try {
                AppModel.createDirectoryIfNotExist(model.statePath);

            } catch (Exception ex) {
                logger.errorf(ex, "Не удалось создать каталог для файла состояния! (%s)", model.statePath);
            }
        }

        if (isUI) out.cursorOff().attr(0).clear().at(1, 1);

        new Thread(() -> {
            // Крутим цикл репликации.
            while (!isTerminated) {
                try {
                    model.replicate();
                    if (model.replModel.curRowCount > 0) {
                        logger.infof("Успешная репликация: время=%s, строк=%d",
                                formatHHMMSS(ChronoUnit.MILLIS.between(model.replModel.curStartTime, model.replModel.curEndTime)),
                                model.replModel.curRowCount);
                        for (TabInfo t : model.replModel.tabs) {
                            if (t.count > 0) {
                                logger.infof("  %s: время=%s строк=%d записано=%d", t.name,
                                        formatHHMMSS(ChronoUnit.MILLIS.between(t.startTime, t.endTime)),
                                        t.count, t.writed);
                            }
                        }
                    }

                } catch (Exception ex) {
                    logger.errorf(ex, "Ошибка репликации!");
                    //break;
                }
                // Пауза между репликациями.
                if (safeTermSleep(model.replModel.delayTime)) break;
            }
            isTerminated = true;
        }).start();

        while (!isTerminated) {
            updateState();
            safeTermSleep(model.redrawInterval);
        }

        if (isUI) out.cursorOn();
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

    private static ServiceModel CM = new ServiceModel(0);

    private static int bgtitle = 19, bgbase = 17;

    static String trunc(String s, int len) {
        int n = s.length();
        if (n <= len) return s;
        return s.substring(0, len);
    }

    static long millis(LocalDateTime t1, LocalDateTime t2) {
        return ChronoUnit.MILLIS.between(t1, t2);
    }

    public static void updateState() {
        stateUpdateTime = LocalDateTime.now();
        model.replModel.copyTo(CM);
        //
        String h1 = "---", h2 = "---", h3 = "---";
        String s1 = "---", s2 = "---";
        if (CM.startTime != null) {
            h1 = fmtDT86(CM.startTime) + "  (пауза между: " + CM.delayTime + " мс)";
            h2 = formatHHMMSS(millis(CM.startTime, stateUpdateTime));
            h3 = String.format("%d, время %s", CM.allCount, formatHHMMSS(CM.allMsec));

            if (CM.isReplication) {
                s1 = String.format("%s", fmtDT86(CM.curStartTime));
                s2 = String.format("%s", formatHHMMSS(millis(CM.curStartTime, stateUpdateTime)));
            } else {
                if (CM.curStartTime != null) {
                    s1 = String.format("%s, Завершение: %s", fmtDT86(CM.curStartTime), fmtDT86(CM.curEndTime));
                    s2 = String.format("%s", formatHHMMSS(millis(CM.curStartTime, CM.curEndTime)));
                }
                if (CM.curEndTime != null) {
                    LocalDateTime enddelay = CM.curEndTime.plusNanos(CM.delayTime * 1000000L);
                    long tm = millis(stateUpdateTime, enddelay);
                    h3 = h3 + "  (повтор через: " + formatHHMMSS(tm) + ")";
                }
            }
        }

        if (isUI) {
            //
            out.reset().at(1, 1);
            out.color(15, bgtitle).bold().print(w, " «Сервис репликации БД»").boldOff()
                    .atX(w - 19).bold().color(230).println(fmtDT86(stateUpdateTime)).boldOff();
            out.color(45).print(w, " v2019.10.12").atX(w - 46).color(123).print("© Докшин Алексей Николаевич, ")
                    .color(49).underline().println("dokshin@gmail.com").underlineOff();
            out.color(bgbase, 18).println(delim3_4).color(7, bgbase);

            int c1 = 18, c2 = 19;
            //
            out.println(w, " Время начала работы : %s", h1);
            out.println(w, " Общее время работы  : %s", h2);
            out.println(w, " Репликаций          : %s", h3);

            out.color(c1).println(delimSB).color(7, bgbase);
            out.println(w, CM.isReplication ? " Текущая репликация" : " Последняя репликация");
            out.println(w, "   Начало: %s", s1);
            out.println(w, "   Длительность: %s", s2);

            out.color(c1).println(delimS).color(7, bgbase);
            for (int i = 0; i < CM.tabs.length; i++) {
                TabInfo tab = CM.tabs[i];
                if (CM.curTab == i) {
                    long percent = tab.count == 0 ? 0 : tab.index * 100L / tab.count;
                    out.bold().color(11, 20).print(w, " ▶ ");
                    out.color(15).print("Таблица: %s (обработка)", tab.name);
                    out.atX(w - 7 - 6 - 28).print("%27s", String.format("[%d/%d:%d]", tab.index, tab.count, tab.writed));
                    out.atX(w - 7 - 6).print("%3d%%", percent);
                    out.atX(w - 7).print("%s", formatHHMMSS(millis(tab.startTime, stateUpdateTime)));
                    out.boldOff();
                } else {
                    if (tab.startTime != null) { // ○◉□▣
                        long percent = tab.count == 0 ? 100 : tab.index * 100L / tab.count;
                        if (tab.isError()) {
                            out.color(1, bgbase).print(w, " ▣ ");
                        } else {
                            out.color(10, bgbase).print(w, " ▣ ");
                        }
                        out.color(7).print("Таблица: %s", tab.name);
                        out.atX(w - 7 - 6 - 28).print("%27s", String.format("[%d/%d:%d]", tab.index, tab.count, tab.writed));
                        out.atX(w - 7 - 6).print("%3d%%", percent);
                        out.atX(w - 7).print("%s", formatHHMMSS(millis(tab.startTime, tab.endTime)));
                    } else {
                        out.color(21, bgbase).print(w, " □ ").color(7).print("Таблица: %s", tab.name);
                    }
                }
                out.println();
            }

            if (CM.errMessage == null) {
                out.color(7, bgbase).println(w, "");
            } else {
                out.color(7, 88).print(w, " Ошибка : ")
                        .color(228).println(trunc(CM.errMessage, w - 10)).color(7, bgbase);
            }
            out.color(18, bgbase).println(delim1_4).reset();
        }

        // В файл...
        if (!isUI) {
            try {
                //model.createDirectoryIfNotExist("./state/replicator");
                StringTools.TextBuilder b = new StringTools.TextBuilder();
                b.println(" «Сервис репликации БД»                                     %s", fmtDT86(stateUpdateTime));
                b.println(" v2019.10.12                     © Докшин Алексей Николаевич, dokshin@gmail.com");
                b.println("--------------------------------------------------------------------------------");
                b.println(" Время начала работы : %s", h1);
                b.println(" Общее время работы  : %s", h2);
                b.println(" Репликаций          : %s", h3);
                b.println("--------------------------------------------------------------------------------");
                b.println(CM.isReplication ? " Текущая репликация" : " Последняя репликация");
                b.println("   Начало: %s", s1);
                b.println("   Длительность: %s", s2);
                b.println("--------------------------------------------------------------------------------");
                for (int i = 0; i < CM.tabs.length; i++) {
                    TabInfo tab = CM.tabs[i];
                    if (CM.curTab == i) {
                        long percent = tab.count == 0 ? 0 : tab.index * 100L / tab.count;
                        b.println("[>] Таблица: %-20s %32s %3d%% %s",
                                tab.name, String.format("[%d/%d]", tab.index, tab.count), percent,
                                formatHHMMSS(millis(tab.startTime, stateUpdateTime)));
                    } else {
                        if (tab.startTime != null) { // ○◉□▣
                            long percent = tab.count == 0 ? 100 : tab.index * 100L / tab.count;
                            b.println("[%s] Таблица: %-20s %32s %3d%% %s",
                                    tab.isError() ? "E" : "+",
                                    tab.name, String.format("[%d/%d:%d]", tab.index, tab.count, tab.writed), percent,
                                    formatHHMMSS(millis(tab.startTime, tab.endTime)));
                        } else {
                            b.println("[ ] Таблица: %s", tab.name);
                        }
                    }
                }
                b.println("--------------------------------------------------------------------------------");
                if (CM.errMessage != null) b.println(" Ошибка : %s", CM.errMessage);

                FileWriter fw = new FileWriter(model.statePath + File.separator + "app.state");
                fw.append(b.toString());
                fw.flush();
                fw.close();

            } catch (Exception ignore) {
            }
        }

    }

    private static void stopApp() {
        if (isUI) out.reset().color(7, 0).clear().cursorOn();
        logger.infof("Приложение завершено!");
    }

}