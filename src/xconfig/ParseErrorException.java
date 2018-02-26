/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package xconfig;

/**
 * Ошибка разбора конфигурации.
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class ParseErrorException extends Exception {

    public ParseErrorException(String message) {
        super(message);
    }
}
