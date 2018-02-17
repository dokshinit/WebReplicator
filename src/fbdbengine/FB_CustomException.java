/*
 * Copyright (c) 2013, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package fbdbengine;

import org.firebirdsql.jdbc.FBSQLException;

/**
 * Класс исключения сгенерированного вручную в БД.
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class FB_CustomException extends Exception {

    // Код исключения.
    public int id;
    // Имя исключения.
    public String name;
    // Сообщение исключения (пока не решен вопрос корректного отображения русских букв!).
    public String message;
    // Оригинальное исключение (которое парсилось).
    public FBSQLException original;

    /**
     * Проверка исключения на то, что оно является пользовательским исключением
     * в ДБ.
     *
     * @param ex Исключение.
     * @return Результат проверки: true - является, false - нет.
     */
    public static boolean check(Exception ex) {
        if (ex instanceof FBSQLException) {
            FBSQLException e = (FBSQLException) ex;
            if (e.getErrorCode() == 335544517) {
                return true;
            }
        }
        return false;
    }

    /**
     * Распознавание пользовательского исключения в ДБ.
     *
     * @param ex Исключение.
     * @return Если пользовательское исключение - экземпляр класса с
     * параметрами, если нет - null.
     */
    public static FB_CustomException parse(Exception ex) {
        try {
            if (check(ex)) {
                FB_CustomException ce = new FB_CustomException();
                ce.original = (FBSQLException) ex;
                String s = ce.original.getCause().getMessage(); // .getInternalException().
                int n = s.indexOf("\n", 10);
                if (n > 0) {
                    ce.id = Integer.parseInt(s.substring(10, n).trim());
                }
                int m = s.indexOf("\n", n + 1);
                if (m > 0) {
                    ce.name = s.substring(n + 1, m).trim();
                }
                int p = s.indexOf("\n", m + 1);
                if (p > 0) {
                    try {
                        ce.message = s.substring(m + 1, p).trim();
                    } catch (Exception ignored) {
                    }
                }
                return ce;
            }
        } catch (Exception ignored) {
        }
        return null;
    }
}
