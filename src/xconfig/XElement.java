/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package xconfig;

/**
 * Базовый элемент конфигурационного файла.
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class XElement {

    /** Родитель элемента. */
    private final XNode parent;
    /** Имя элемента. */
    private String name;

    /**
     * Конструктор.
     *
     * @param parent Родитель.
     * @param name   Имя.
     */
    protected XElement(XNode parent, String name) {
        this.parent = parent;
        this.name = name;
    }

    /**
     * Получение владельца элемента.
     *
     * @return Родитель элемента.
     */
    public XNode getParent() {
        return parent;
    }

    /**
     * Получение имени элемента.
     *
     * @return Имя элемента.
     */
    public String getName() {
        return name;
    }

    /**
     * Изменение имени элемента. Только для служебного использования из потомка!
     *
     * @param name Имя.
     */
    protected void setName(String name) {
        this.name = name;
    }
}
