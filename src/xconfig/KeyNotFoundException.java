/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package xconfig;

/**
 * Ошибка при отсутствии ключа.
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class KeyNotFoundException extends Exception {

    public KeyNotFoundException(String message) {
        super(message);
    }
}
