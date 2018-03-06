/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */

package app;

import java.io.File;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ResourceBundle;
import java.util.function.Supplier;
import java.util.logging.*;

/**
 * Расширенный логгер. Правильно определяет точку вызова (производит анализ стека - выбирает первый элемент лежащий
 * после методов этого класса и его предка). Ради этого правильного определения точки вызова и был написан! Также
 * реализован механизм исключения помеченных методов\классов при определении точки вызова.
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
@LoggerExt.ExcludeClass(false)
public final class LoggerExt extends Logger {

    /**
     * Анотация, позволяющая задать правила логирования для класса. Если value=true - все методы класса исключаются из
     * логирования, если value=false - исключаются только методы помеченные анотацией @ExcludeMethod.
     */
    @Target(ElementType.TYPE)
    @Retention(value = RetentionPolicy.RUNTIME)
    public @interface ExcludeClass {

        /**
         * Режим логирования.
         *
         * @return true - все методы класса исключаются из логирования, false - исключаются только методы помеченные
         * анотацией @ExcludeMethod.
         */
        boolean value();
    }

    @Target(ElementType.METHOD)
    @Retention(value = RetentionPolicy.RUNTIME)
    public @interface ExcludeMethod {
    }

    private static final LoggerExt logger = new LoggerExt("app");

    private boolean isEnabled; // true-идёт логирование, false-нет.
    private boolean isCallerFind; // true-производится поиск источника точки вызова лога, false-нет.

    private FileHandler fileHandler; // Хендл при выводе в файл.
    private String filePath, filePattern; // Путь и шаблон имени файла.
    private LocalDateTime fileDT; // Дата файла подставленная в текущий паттерн.
    private final Object fileSync = new Object(); // Для синхронного доступа при логе в файл.

    @FunctionalInterface
    public interface FileUpdateCheck {
        boolean isNeedUpdate(LoggerExt log);
    }

    private FileUpdateCheck fileCheck;
    public static final FileUpdateCheck FILE_UPDATE_NO = (log) -> false;
    public static final FileUpdateCheck FILE_UPDATE_DATE = (log) -> !log.getFileDT().toLocalDate().equals(LocalDate.now());

    private int mask; // Маска для управления логированием (для пользовательского управления выводом информации в лог), самим логгером не используется!

    private LoggerExt(String name) {
        super(name, null);
        isEnabled = false;
        isCallerFind = true;
        fileHandler = null;
        filePath = "./logs";
        filePattern = "%1$s_%2$s.log";
        fileCheck = FILE_UPDATE_DATE;
        mask = -1;
        LogManager.getLogManager().addLogger(LoggerExt.this);
        setLevel(Level.ALL);
    }

    public static LoggerExt getCommonLogger() {
        return logger;
    }

    public static LoggerExt getNewLogger(String name) {
        return new LoggerExt(name);
    }

    public LoggerExt enable(boolean isEnabled) {
        this.isEnabled = isEnabled;
        return this;
    }

    public boolean isEnabled() {
        return isEnabled;
    }

    public LoggerExt mask(int mask) {
        this.mask = mask;
        return this;
    }

    public int getMask() {
        return mask;
    }

    public boolean isMask(int mask) {
        return (this.mask & mask) != 0;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    public static void setConsoleFormatter(Formatter formatter) {
        // Переводим консольный логгер на наш форматер.
        Logger rootLogger = Logger.getLogger("");
        Handler[] handlers = rootLogger.getHandlers();
        if (handlers[0] instanceof ConsoleHandler) {
            handlers[0].setFormatter(formatter);
            handlers[0].setLevel(Level.ALL);
            if (isWindowsOS()) {
                // Удаление консольного логирования в винде! Там и с кодировками проблемы и не нужно!
                rootLogger.removeHandler(handlers[0]);
            }
        }
    }

    public static void setConsoleFormatter() {
        LoggerExt.setConsoleFormatter(new LogFormatter(false, false, true));
    }

    public static void removeConsoleOutput() {
        Logger rootLogger = Logger.getLogger("");
        Handler[] handlers = rootLogger.getHandlers();
        if (handlers[0] instanceof ConsoleHandler) rootLogger.removeHandler(handlers[0]);
    }

    public static String getOSName3() {
        String s = System.getProperty("os.name", "").toLowerCase();
        return s.length() < 3 ? "" : s.substring(0, 3);
    }

    public static boolean isWindowsOS() {
        return "win".equals(getOSName3());
    }

    public static boolean isLinuxOS() {
        return "lin".equals(getOSName3());
    }

    private static final SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

    /**
     * Создание каталога, если он еще не существует.
     *
     * @param path Имя каталога.
     * @return Файл каталога.
     * @throws ExError
     */
    private static File createDirectoryIfNotExist(String path) throws ExError {
        try {
            File dir = new File(path);
            if (!dir.exists())
                if (!dir.mkdir()) throw new ExError("Ошибка создания каталога!");
            return dir;
        } catch (ExError ex) {
            throw ex;
        } catch (Exception e) {
            throw new ExError(e, "Ошибка создания каталога!");
        }
    }

    public String getFilePath() {
        return filePath;
    }

    public String getFilePattern() {
        return filePattern;
    }

    public LocalDateTime getFileDT() {
        return fileDT;
    }

    public FileUpdateCheck getFileCheck() {
        return fileCheck;
    }

    public boolean isToFile() {
        synchronized (fileSync) {
            return fileHandler != null;
        }
    }

    public static final DateTimeFormatter FMT_DATETIME_YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");

    public LoggerExt toFile(final String path, String pattern, FileUpdateCheck check) {
        synchronized (fileSync) {
            Level oldLevel = getLevel();
            setLevel(Level.OFF);

            // Если вывод уже ведется в файл - закрываем предыдущий файл.
            if (fileHandler != null) close();

            // Настраиваем логгер для файлового вывода.
            try {
                filePath = path;
                filePattern = pattern;
                fileDT = LocalDateTime.now();
                fileCheck = check;
                String filename = filePath + "/" + String.format(filePattern, getName(), FMT_DATETIME_YYYYMMDD.format(fileDT), fileDT);
                createDirectoryIfNotExist(path);
                // В паттерне разделители заменяются на локальные!
                fileHandler = new FileHandler(filename, 0, 1, true);
                fileHandler.setLevel(Level.ALL);
                fileHandler.setFormatter(new LogFormatter());
                addHandler(fileHandler);

            } catch (Exception e) {
                fileHandler = null;
                error("Ошибка создания лог-файла! Вывод в лог-файл будет игнорироваться!", e);
            }

            setLevel(oldLevel);
        }
        return this;
    }

    // В файл с текущими настройками (если не менялсь - дефолтными).
    public LoggerExt toFile() {
        return toFile(filePath, filePattern, fileCheck);
    }

    public LoggerExt flush() {
        synchronized (fileSync) {
            if (fileHandler != null) {
                try {
                    fileHandler.flush();
                } catch (Exception e) {
                    error("Ошибка сохранения буферов лог-файла!", e);
                }
            }
        }
        return this;
    }

    public LoggerExt close() {
        synchronized (fileSync) {
            if (fileHandler != null) {
                try {
                    fileHandler.flush();
                    removeHandler(fileHandler);
                    fileHandler.close();
                    fileHandler = null;

                } catch (Exception e) {
                    fileHandler = null;
                    error("Ошибка закрытия лог-файла!", e);
                }
            }
        }
        return this;
    }

    public LoggerExt updateFileIfNeed() {
        // Для того, чтобы блок синхронизации задействовался только тогда, когда надо - помещаем внутрь.
        if (fileCheck != null && fileCheck.isNeedUpdate(this)) {
            synchronized (fileSync) {
                if (fileHandler != null) toFile();
            }
        }
        return this;
    }

    protected StackTraceElement findCallerPoint() {
        final StackTraceElement[] stack = new Throwable().getStackTrace();
        boolean isinnerlevel = true;
        for (StackTraceElement e : stack) {
            boolean islog = false;
            try {
                Class clazz = Class.forName(e.getClassName());
                if (islog = Logger.class.isAssignableFrom(clazz)) {
                    // Если основа - логгер, то все методы не могут быть точками вызова.
                    islog = true;
                } else if (clazz.isAnnotationPresent(ExcludeClass.class)) {
                    // Если класс имеет анотацию с правилами логировани.
                    ExcludeClass eca = (ExcludeClass) clazz.getAnnotation(ExcludeClass.class);
                    if (eca.value()) {
                        // Если помечен как логгер, то все методы не могут быть точками вызова.
                        islog = true;
                    } else {
                        // Если не помечен как логгер, то проверяем методы на наличие анотации.
                        String name = e.getMethodName();
                        for (Method m : clazz.getMethods()) {
                            if (name.equals(m.getName())) {
                                // Если совпало имя - проверяем на анотацию.
                                if (m.isAnnotationPresent(ExcludeMethod.class)) {
                                    // Если есть анотация, то метод не может быть точкой вызова.
                                    islog = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            } catch (ClassNotFoundException ignore) {
            }
            if (isinnerlevel) {
                if (islog) isinnerlevel = false;
            } else {
                if (!islog) return e;
            }
        }
        return null;
    }

    @Override
    public void log(LogRecord record) {
        if (isEnabled) {
            updateFileIfNeed(); // Помещаем только сюда, т.к. этот метод юзается всеми остальными.
            if (isCallerFind) {
                StackTraceElement e = findCallerPoint();
                if (e != null) {
                    record.setSourceClassName(e.getClassName());
                    record.setSourceMethodName(e.getMethodName());
                }
            }
            super.log(record);
        }
    }

    @Override
    public void throwing(String sourceClass, String sourceMethod, Throwable thrown) {
        if (isEnabled) {
            isCallerFind = false;
            super.throwing(sourceClass, sourceMethod, thrown);
        }
    }

    @Override
    public void exiting(String sourceClass, String sourceMethod, Object result) {
        if (isEnabled) {
            isCallerFind = false;
            super.exiting(sourceClass, sourceMethod, result);
        }
    }

    @Override
    public void exiting(String sourceClass, String sourceMethod) {
        if (isEnabled) {
            isCallerFind = false;
            super.exiting(sourceClass, sourceMethod);
        }
    }

    @Override
    public void entering(String sourceClass, String sourceMethod, Object[] params) {
        if (isEnabled) {
            isCallerFind = false;
            super.entering(sourceClass, sourceMethod, params);
        }
    }

    @Override
    public void entering(String sourceClass, String sourceMethod, Object param1) {
        if (isEnabled) {
            isCallerFind = false;
            super.entering(sourceClass, sourceMethod, param1);
        }
    }

    @Override
    public void entering(String sourceClass, String sourceMethod) {
        if (isEnabled) {
            isCallerFind = false;
            super.entering(sourceClass, sourceMethod);
        }
    }

    @Override
    public void logrb(Level level, String sourceClass, String sourceMethod, ResourceBundle bundle, String msg, Throwable thrown) {
        if (isEnabled) {
            isCallerFind = false;
            super.logrb(level, sourceClass, sourceMethod, bundle, msg, thrown);
        }
    }

    @Override
    public void logp(Level level, String sourceClass, String sourceMethod, Throwable thrown, Supplier<String> msgSupplier) {
        if (isEnabled) {
            isCallerFind = false;
            super.logp(level, sourceClass, sourceMethod, thrown, msgSupplier);
        }
    }

    @Override
    public void logp(Level level, String sourceClass, String sourceMethod, String msg, Throwable thrown) {
        if (isEnabled) {
            isCallerFind = false;
            super.logp(level, sourceClass, sourceMethod, msg, thrown);
        }
    }

    @Override
    public void logp(Level level, String sourceClass, String sourceMethod, String msg, Object[] params) {
        if (isEnabled) {
            isCallerFind = false;
            super.logp(level, sourceClass, sourceMethod, msg, params);
        }
    }

    @Override
    public void logp(Level level, String sourceClass, String sourceMethod, String msg, Object param1) {
        if (isEnabled) {
            isCallerFind = false;
            super.logp(level, sourceClass, sourceMethod, msg, param1);
        }
    }

    @Override
    public void logp(Level level, String sourceClass, String sourceMethod, Supplier<String> msgSupplier) {
        if (isEnabled) {
            isCallerFind = false;
            super.logp(level, sourceClass, sourceMethod, msgSupplier);
        }
    }

    @Override
    public void logp(Level level, String sourceClass, String sourceMethod, String msg) {
        if (isEnabled) {
            isCallerFind = false;
            super.logp(level, sourceClass, sourceMethod, msg);
        }
    }

    public void configf(String fmt, Object... params) {
        if (isEnabled) {
            super.config(String.format(fmt, params));
        }
    }

    public void infof(String fmt, Object... params) {
        if (isEnabled) {
            super.info(String.format(fmt, params));
        }
    }

    public void warning(String message, Throwable w) {
        if (isEnabled) {
            super.log(Level.WARNING, message, w);
        }
    }

    public void warningf(String fmt, Object... params) {
        if (isEnabled) {
            super.warning(String.format(fmt, params));
        }
    }

    public void warningf(Throwable w, String fmt, Object... params) {
        if (isEnabled) {
            super.log(Level.WARNING, String.format(fmt, params), w);
        }
    }

    public void error(String message) {
        if (isEnabled) {
            super.log(Level.SEVERE, message);
        }
    }

    public void error(String message, Throwable w) {
        if (isEnabled) {
            super.log(Level.SEVERE, message, w);
        }
    }

    public void errorf(String fmt, Object... params) {
        if (isEnabled) {
            super.log(Level.SEVERE, String.format(fmt, params));
        }
    }

    public void errorf(Throwable w, String fmt, Object... params) {
        if (isEnabled) {
            super.log(Level.SEVERE, String.format(fmt, params), w);
        }
    }
}
