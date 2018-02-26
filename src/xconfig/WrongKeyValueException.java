/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package xconfig;

/**
 * Ошибка при неверном значении ключа.
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class WrongKeyValueException extends Exception {

    public WrongKeyValueException(String message) {
        super(message);
    }
}
