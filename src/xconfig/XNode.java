/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package xconfig;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Узел (как специальный случай узла - массив).
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class XNode extends XElement {

    /** Ссылка на конфигурацию в рамках которой создан элемент. */
    private final XConfig config;

    /** Список атрибутов. */
    private final ArrayList<XAttribute> attributes = new ArrayList<>();
    /** Список дочених узлов. */
    private final ArrayList<XNode> nodes = new ArrayList<>();
    /** Флаг-индикатор того, что узел является массивом. */
    private final boolean isArray;
    /** Ссылка на атрибут размера массива. */
    private XAttribute attrArraySize;

    /**
     * Конструктор корневого узла.
     *
     * @param config Ссылка на конфиг в рамках которого создаётся узел.
     */
    protected XNode(XConfig config) {
        super(null, "");
        this.config = config;
        this.isArray = false;
        this.attrArraySize = null;
    }

    /**
     * Конструктор.
     *
     * @param parent Родитель.
     * @param name   Имя узла.
     */
    @SuppressWarnings("OverridableMethodCallInConstructor")
    protected XNode(XNode parent, String name) {
        super(parent, name);
        config = parent.getConfig(); // Не может быть null! Для корневого узла - другой конструктор!
        isArray = name.endsWith("[]");
        if (isArray) {
            setName(name.substring(0, name.length() - 2));
            attrArraySize = createAttribute("size", "0");
        }
    }

    public XConfig getConfig() {
        return config;
    }

    /**
     * Получение пути до узла.
     *
     * @return Путь до узла.
     */
    public String getPath() {
        return isRoot() ? "" : getParent().getChildNodePath();
    }

    /**
     * Получение имени ключа для узла.
     *
     * @return Имя ключа.
     */
    public String getID() {
        String path = getPath();
        if (isArray) {
            return (path.isEmpty() ? "" : (path + ".")) + getName() + "[]";
        }
        if (!isRoot() && getParent().isArray()) {
            return path + getName();
        }
        return (path.isEmpty() ? "" : (path + ".")) + getName();
    }

    /**
     * Получение пути для дочерних атрибутов.
     *
     * @return Путь для дочерних атрибутов.
     */
    public String getChildAttributePath() {
        return getID();
    }

    /**
     * Получение пути для дочерних узлов.
     *
     * @return Путь для дочерних узлов.
     */
    public String getChildNodePath() {
        String path = getPath();
        if (!isRoot() && getParent().isArray()) {
            return path + getName();
        }
        return (path.isEmpty() ? "" : (path + ".")) + getName();
    }

    public String getChildID(final String key) {
        String path = getChildAttributePath();
        return path.isEmpty() ? key : (path + "." + key);
    }

    public String getChildNodeID(final String nodekey) {
        String path = getChildNodePath();
        return path.isEmpty() ? nodekey : (path + "." + nodekey);
    }

    /**
     * Проверка, является ли узел массивом.
     *
     * @return Результат: true - да, false - нет.
     */
    public boolean isArray() {
        return isArray;
    }

    /**
     * Проверка, является ли узел корневым.
     *
     * @return Результат: true - да, false - нет.
     */
    public boolean isRoot() {
        return getParent() == null;
    }

    /**
     * Получение кол-ва атрибутов узла.
     *
     * @return Кол-во атрибутов узла.
     */
    public int getAttributeCount() {
        return attributes.size();
    }

    /**
     * Получение списка атрибутов узла (только для чтения).
     *
     * @return Список атрибутов.
     */
    public List<XAttribute> getAttributes() {
        return new UnmodificableWrappedList<>(attributes);
    }

    /**
     * Получение кол-ва дочерних нод узла. Для узла-массива - это кол-во элементов массива.
     *
     * @return Кол-во дочерних нод узла.
     */
    public int getNodeCount() {
        return nodes.size();
    }

    /**
     * Получение списка дочерних узлов (только для чтения).
     *
     * @return Список дочерних узлов.
     */
    public List<XNode> getNodes() {
        return new UnmodificableWrappedList<>(nodes);
    }

    /**
     * Добавление атрибута в узел.
     *
     * @param attr Аттрибут.
     * @return Аттрибут.
     */
    protected XAttribute addAttribute(XAttribute attr) {
        config.putAttributeKey(attr.getID(), attr);
        attributes.add(attr);
        return attr;
    }

    /**
     * Создание и добавление атрибута.
     *
     * @param name  Имя атрибута.
     * @param value Значение.
     * @return Аттрибут.
     */
    public XAttribute createAttribute(String name, String value) {
        return addAttribute(new XAttribute(this, name, value));
    }

    /**
     * Добавление дочернего узла в узел.
     *
     * @param node Дочерний узел.
     * @return Дочерний узел.
     */
    protected XNode addNode(XNode node) {
        config.putNodeKey(node.getID(), node);
        nodes.add(node);
        if (isArray) {
            attrArraySize.setValue("" + nodes.size());
        }
        return node;
    }

    /**
     * Создание и добавление дочернего узла.
     *
     * @param name Имя дочернего узла.
     * @return Дочерний узел.
     */
    public XNode createNode(String name) {
        return addNode(new XNode(this, name));
    }

    /**
     * Удаление атрибута узла. Если не принадлежит узлу - игнорируется. ВНИМАНИЕ! Т.к. удаляет элемент из списка
     * атрибутов, то если требуется одновременный обход списка - необходимо использовать итератор (forEachRemaining не
     * подходит, т.к. не позволяет изменять список в callback).
     *
     * @param attr         Дочерний атрибут.
     * @param isfullremove Флаг определяющий необходимость удалить атрибут из списка атрибутов. Введен для возможности
     *                     удалить из списка позже.
     */
    public void removeAttribute(XAttribute attr, boolean isfullremove) {
        if (attr != null && attr.getParent() == this) {
            config.removeAttrKey(attr.getID());
            if (isfullremove) {
                attributes.remove(attr);
            }
        }
    }

    /**
     * Удаление всех атрибутов узла.
     */
    public void removeAttributes() {
        attributes.stream().forEach((attr) -> removeAttribute(attr, false));
        attributes.clear();
    }

    /**
     * Удаление дочернего узла с рекурсивным удалением всех вложенных элементов.
     *
     * @param node         Дочерний узел.
     * @param isfullremove Флаг определяющий необходимость удалить узел из списка узлов. Введен для возможности удалить
     *                     из списка позже.
     */
    public void removeNode(XNode node, boolean isfullremove) {
        if (node != null && node.getParent() == this) {
            config.removeNodeKey(node.getID());
            if (node.isArray()) {
                config.removeAttrKey(node.getID() + ".size");
            }
            if (isfullremove) {
                nodes.remove(node);
            }

            // Рекурсивное удаление элементов узла.
            node.removeAttributes();
            node.removeNodes();
        }
    }

    public void removeNodes() {
        nodes.stream().forEach((node) -> {
            if (node != null && node.getParent() == this) {
                config.removeNodeKey(node.getID());
                if (node.isArray()) {
                    config.removeAttrKey(node.getID() + ".size");
                }

                // Рекурсивное удаление элементов узла.
                node.removeAttributes();
                node.removeNodes();
            }
        });
        nodes.clear();
    }

    /**
     * Очистка (удаление) всех атрибутов узла. Не поизводит их удаление из хранилища ключей! Для удаления с удалением из
     * хранилища необходимо использовать removeAttrs().
     */
    public void clearAttributes() {
        attributes.clear();
    }

    /**
     * Очистка (удаление) всех атрибутов узла. Не поизводит их удаление из хранилища ключей! Для удаления с удалением из
     * хранилища необходимо использовать removeNodes().
     */
    public void clearNodes() {
        nodes.clear();
    }

    public void clear() {
        clearAttributes();
        clearNodes();
    }

    /**
     * Удаление узла из дочерних узлов родителя.
     */
    public void remove() {
        if (!isRoot()) {
            getParent().removeNode(this, true);
        }
    }

    public XAttribute findAttr(String name) {
        for (XAttribute a : attributes) {
            if (a.getName().equals(name)) {
                return a;
            }
        }
        return null;
    }

    public XNode findNode(String name) {
        for (XNode n : nodes) {
            if (n.getName().equals(name)) {
                return n;
            }
        }
        return null;
    }

    public XNode getNode(String childnode) {
        return config.getNode(getChildNodeID(childnode));
    }

    public String getKey(String childkey) throws KeyNotFoundException {
        return config.getKey(getChildID(childkey));
    }

    public String getKey(String childkey, String defaultvalue) {
        return config.getKey(getChildID(childkey), defaultvalue);
    }

    public int getIntKey(final String childkey) throws KeyNotFoundException, WrongKeyValueException {
        return config.getIntKey(getChildID(childkey));
    }

    public int getIntKey(final String childkey, int defaultvalue) throws WrongKeyValueException {
        return config.getIntKey(getChildID(childkey), defaultvalue);
    }

    public Date getDateKey(final String childkey, DateFormat fmt) throws KeyNotFoundException, WrongKeyValueException {
        return config.getDateKey(getChildID(childkey), fmt);
    }

    public Date getDateKey(final String childkey, DateFormat fmt, Date defaultvalue) throws WrongKeyValueException {
        return config.getDateKey(getChildID(childkey), fmt, defaultvalue);
    }

    public BigDecimal getDecimalKey(final String childkey) throws KeyNotFoundException, WrongKeyValueException {
        return config.getDecimalKey(getChildID(childkey));
    }

    public BigDecimal getDecimalKey(final String childkey, BigDecimal defaultvalue) throws WrongKeyValueException {
        return config.getDecimalKey(getChildID(childkey), defaultvalue);
    }
}
