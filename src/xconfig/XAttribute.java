/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package xconfig;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.util.Date;

/**
 * Аттрибут.
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class XAttribute extends XElement {

    /**
     * Значение.
     */
    private String value;

    /**
     * Конструктор.
     *
     * @param parent Родитель.
     * @param name   Имя.
     * @param value  Значение.
     */
    protected XAttribute(XNode parent, String name, String value) {
        super(parent, name);
        assert (parent != null);
        this.value = value;
    }

    /**
     * Получение пути до аттрибута.
     *
     * @return Путь до аттрибута.
     */
    public String getPath() {
        return getParent().getChildAttributePath();
    }

    /**
     * Получение имени ключа для аттрибута.
     *
     * @return Имя ключа.
     */
    public String getID() {
        return getParent().getChildID(getName());
    }

    /**
     * Получение значения аттрибута.
     *
     * @return Значение.
     */
    public String getValue() {
        return value;
    }

    /**
     * Установка значения аттрибута.
     *
     * @param value Значение.
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Получение строки с маскировкой кавычек.
     *
     * @param str Исходная строка.
     * @return Строка с маскировкой спецсимволов.
     */
    public static String getMaskedString(String str) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, len = str.length(); i < len; i++) {
            char ch = str.charAt(i);
            if (ch == '\"') {
                sb.append("\\");
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    /**
     * Получение значения аттрибута с маскировкой спецсимволов (кавычек).
     *
     * @return Значение с маскировкой спецсимволов.
     */
    public String getMaskedValue() {
        return getMaskedString(value);
    }

    public void remove() {
        getParent().removeAttribute(this, true);
    }

    public int getValueAsInt() throws WrongKeyValueException {
        if (value == null || value.isEmpty()) {
            throw new WrongKeyValueException("Attr value is not int! [" + getParent().getChildID(getName()) + "]: empty value!");
        }
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            throw new WrongKeyValueException("Attr value is not int [" + getParent().getChildID(getName()) + "]: wrong value [" + value + "]!");
        }
    }

    public BigDecimal getValueAsDecimal() throws WrongKeyValueException {
        if (value == null || value.isEmpty()) {
            throw new WrongKeyValueException("Attr value is not BigDecimal! [" + getParent().getChildID(getName()) + "]: empty value!");
        }
        try {
            return new BigDecimal(value);
        } catch (Exception e) {
            throw new WrongKeyValueException("Attr value is not BigDecimal [" + getParent().getChildID(getName()) + "]: wrong value [" + value + "]!");
        }
    }

    public Date getValueAsDate(DateFormat df) throws WrongKeyValueException {
        if (value == null || value.isEmpty()) {
            throw new WrongKeyValueException("Attr value is not Date! [" + getParent().getChildID(getName()) + "]: empty value!");
        }
        try {
            return df.parse(value);
        } catch (Exception e) {
            throw new WrongKeyValueException("Attr value is not Date [" + getParent().getChildID(getName()) + "]: wrong value [" + value + "]!");
        }
    }
}
