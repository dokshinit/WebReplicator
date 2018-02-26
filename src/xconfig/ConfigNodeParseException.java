/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package xconfig;

/**
 * Ошибка разбора узла.
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class ConfigNodeParseException extends Exception {

    public ConfigNodeParseException(String message) {
        super(message);
    }
}
